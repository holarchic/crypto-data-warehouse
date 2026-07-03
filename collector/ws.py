"""Generic WebSocket collector engine — the first streaming surface in a poll-only codebase.

`ws_loop` drives a persistent WS connection for one feed: connection lifecycle,
exponential reconnect/backoff, per-symbol subscription management (re-subscribed when
discovery changes the active set), batch-insert, and a connection-timer liveness
heartbeat. Every venue-specific detail is injected via `WSFeedSpec` so the engine
carries zero venue branches — Binance/Bybit × liquidations/mark all reuse it.

This module builds the engine only. Real venue specs, the `liquidations` table, and the
`run()` wiring land in T-009.2 / .4 — nothing here is wired into `main.run()` yet.
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from backoff import backoff_sleep
from config import ExchangeConfig
from status import query_active_pairs, upsert_loop_heartbeat
from websocket import (
    WebSocketConnectionClosedException,
    WebSocketException,
    WebSocketTimeoutException,
    create_connection,
)

from db import connect_db

log = logging.getLogger(__name__)

# --- Engine tunables -------------------------------------------------------
WS_CONNECT_TIMEOUT_S = 10.0    # connect/handshake ceiling — a blackholed endpoint must not wedge
WS_RECV_TIMEOUT_S = 1.0        # per-recv socket timeout, so timers are serviced within ~1s when quiet
WS_HEARTBEAT_INTERVAL_S = 60   # connection-timer liveness upsert while the socket is up
WS_RESUB_CHECK_INTERVAL_S = 60  # re-query active pairs + diff topics on this cadence
WS_BATCH_INTERVAL_S = 1.0      # flush the event buffer at least this often
WS_BATCH_MAX = 500             # ...or sooner when this many events are buffered
WS_MAX_BUFFER = 5000           # hard cap: drop oldest beyond this when a DB outage retains events

# A dropped socket / stalled handshake — reconnect+backoff. NOT backoff.RETRIABLE_EXCEPTIONS
# (that set is ccxt/psycopg-shaped and would never catch a WS drop). WebSocketTimeoutException
# is a WebSocketException subclass, so it is caught explicitly as "quiet" before this set applies.
WS_RETRIABLE = (WebSocketConnectionClosedException, WebSocketException, OSError)


@dataclass
class WSFeedSpec:
    """Venue-variation record — pure data, no logic. Every field maps to a doc-verified
    requirement of a consumer (Binance/Bybit × liquidations/mark).

    Binance liquidations: stream is in the URL (`topics_for_pairs`/`subscribe_frames` → empty),
    no app-level ping (protocol pongs only), proactive reconnect at 24h.
    Bybit liquidations: per-symbol `allLiquidation.{sym}` topics, batched JSON subscribe (venue
    owns its ≤10-arg cap inside `subscribe_frames`), 20s ping, no proactive reconnect.
    """

    name: str                                             # feed label for logs/thread name (NOT a DB key)
    loop_tf: str                                          # liveness heartbeat pseudo-TF (config.LOOP_INTERVALS key)
    url: str                                              # WS endpoint
    topics_for_pairs: Callable[[list[dict]], list[str]]   # active pairs → topic list (constant for Binance)
    subscribe_frames: Callable[[list[str]], list[str]]    # topics → subscribe messages (batched; venue owns arg cap)
    unsubscribe_frames: Callable[[list[str]], list[str]]  # topics → unsubscribe messages
    parse_message: Callable[[str], list[dict]]            # raw frame → event dicts ([] for control/ignored frames)
    insert_batch: Callable[[object, list[dict]], int]     # (conn, events) → inserted; all-or-nothing on one txn
    ping_frame: str | None = None                         # app-level ping payload (None → rely on protocol pings)
    ping_interval_s: float | None = None                  # ping cadence (None → no app ping)
    proactive_reconnect_s: float | None = None            # reconnect before a server keepalive limit (None → never)


@dataclass
class _WSRuntime:
    """Cross-session state owned by `ws_loop`. `conn` is rebindable (mid-session DB reconnect);
    `buffer` persists across reconnects so a batch retained through a DB outage survives a socket
    drop; `error_count` drives the reconnect backoff and resets once a session is established.
    """

    conn: object | None
    buffer: list[dict]
    error_count: int = 0


# ---------------------------------------------------------------------------
# DB touchpoints — each ensures a live conn first; none may crash the session.
# ---------------------------------------------------------------------------


def _ensure_conn(runtime: _WSRuntime, dsn: str) -> bool:
    """Rebind `runtime.conn` if missing/closed. Returns False if reconnect fails (caller skips
    its DB touch and stays connected — the socket must survive a DB blip)."""
    if runtime.conn is not None and not runtime.conn.closed:
        return True
    try:
        runtime.conn = connect_db(dsn)
        return True
    except RuntimeError:
        runtime.conn = None
        return False


def _guarded_rollback(runtime: _WSRuntime) -> None:
    if runtime.conn is not None and not runtime.conn.closed:
        try:
            runtime.conn.rollback()
        except Exception:
            pass


def _active_pairs(runtime: _WSRuntime, config: ExchangeConfig, dsn: str, thread_name: str) -> list[dict] | None:
    """Active pairs for topic derivation, or None on DB failure (distinct from [] = no pairs).

    Commits immediately after the read: `connect_db` is NOT autocommit, so a bare SELECT would
    leave the long-lived, mostly-idle WS connection idle-in-transaction between heartbeats.
    """
    if not _ensure_conn(runtime, dsn):
        return None
    try:
        pairs = query_active_pairs(runtime.conn, config.name, "5m")
        runtime.conn.commit()
        return pairs
    except Exception:
        log.exception("[%s] query_active_pairs failed", thread_name)
        _guarded_rollback(runtime)
        return None


def _heartbeat(runtime: _WSRuntime, config: ExchangeConfig, spec: WSFeedSpec, dsn: str, thread_name: str) -> None:
    """Connection-timer liveness upsert. Guarded so a write hiccup can't kill the loop."""
    if not _ensure_conn(runtime, dsn):
        log.warning("[%s] Heartbeat skipped — no DB conn", thread_name)
        return
    try:
        upsert_loop_heartbeat(runtime.conn, config.name, config.market_type, spec.loop_tf)
    except Exception:
        log.exception("[%s] Heartbeat upsert failed", thread_name)
        _guarded_rollback(runtime)


def _cap_buffer(runtime: _WSRuntime, thread_name: str) -> None:
    overflow = len(runtime.buffer) - WS_MAX_BUFFER
    if overflow > 0:
        del runtime.buffer[:overflow]
        log.warning("[%s] Buffer over cap — dropped %d oldest events", thread_name, overflow)


def _flush(runtime: _WSRuntime, spec: WSFeedSpec, dsn: str, thread_name: str) -> None:
    """Persist the buffer via `spec.insert_batch`. On failure: rollback, retain the buffer (bounded
    at WS_MAX_BUFFER, drop oldest) and retry next flush — the socket stays up."""
    if not runtime.buffer:
        return
    if not _ensure_conn(runtime, dsn):
        log.warning("[%s] Flush skipped — no DB conn (%d buffered)", thread_name, len(runtime.buffer))
        _cap_buffer(runtime, thread_name)
        return
    try:
        inserted = spec.insert_batch(runtime.conn, list(runtime.buffer))
        runtime.buffer.clear()
        log.debug("[%s] Flushed %d events", thread_name, inserted)
    except Exception:
        log.exception("[%s] insert_batch failed — retaining %d buffered", thread_name, len(runtime.buffer))
        _guarded_rollback(runtime)
        _cap_buffer(runtime, thread_name)


# ---------------------------------------------------------------------------
# Subscription management — the engine diffs topic sets; venue callables own framing/arg-cap.
# ---------------------------------------------------------------------------


def _subscribe_initial(ws, runtime: _WSRuntime, spec: WSFeedSpec, config: ExchangeConfig, dsn: str, thread_name: str) -> set[str]:
    """Send the initial subscribe frames for the current active-pair topics. A DB failure yields
    empty topics — do NOT tear down a healthy socket (a pairs-independent feed keeps flowing; a
    per-symbol feed recovers via resubscribe when the DB returns)."""
    pairs = _active_pairs(runtime, config, dsn, thread_name)
    topics = spec.topics_for_pairs(pairs if pairs is not None else [])
    for frame in spec.subscribe_frames(topics):
        ws.send(frame)
    log.info("[%s] Subscribed to %d topics", thread_name, len(topics))
    return set(topics)


def _resubscribe(ws, runtime: _WSRuntime, spec: WSFeedSpec, config: ExchangeConfig, dsn: str, current: set[str], thread_name: str) -> set[str]:
    """Re-query active pairs and diff the topic set; subscribe added, unsubscribe removed. On DB
    failure keep the current set (a transient blip must not drop live subscriptions)."""
    pairs = _active_pairs(runtime, config, dsn, thread_name)
    if pairs is None:
        return current
    new_topics = set(spec.topics_for_pairs(pairs))
    added = new_topics - current
    removed = current - new_topics
    for frame in spec.subscribe_frames(sorted(added)):
        ws.send(frame)
    for frame in spec.unsubscribe_frames(sorted(removed)):
        ws.send(frame)
    if added or removed:
        log.info("[%s] Resubscribe: +%d -%d topics (now %d)", thread_name, len(added), len(removed), len(new_topics))
    return new_topics


def _safe_close(ws) -> None:
    """Non-blocking release — `close()` defaults to a ~3s peer-close wait that would tax reconnect
    timing. Best-effort close(timeout=0), then unconditional shutdown so a graceful-close path
    (recv=="" with connected=False) can't leave the socket allocated."""
    try:
        if ws.connected:
            ws.close(timeout=0)
    except Exception:
        pass
    try:
        ws.shutdown()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Session + loop
# ---------------------------------------------------------------------------


def _run_session(ws, runtime: _WSRuntime, spec: WSFeedSpec, config: ExchangeConfig, dsn: str, thread_name: str, clock: Callable[[], float]) -> None:
    """One connected session: subscribe, then recv+service-timers until a graceful close or the
    proactive-reconnect deadline (returns), or a drop (raises WS_RETRIABLE to the outer loop)."""
    ws.settimeout(WS_RECV_TIMEOUT_S)
    topics = _subscribe_initial(ws, runtime, spec, config, dsn, thread_name)
    runtime.error_count = 0  # connection established + subscribed → a later independent drop backs off from step 1

    now = clock()
    conn_start = now
    t_flush = t_ping = t_heartbeat = t_resub = now

    while True:
        try:
            raw = ws.recv()
        except WebSocketTimeoutException:
            raw = None  # quiet tick — service timers, not a drop

        if raw == "" and not ws.connected:
            log.info("[%s] Server closed the connection (graceful)", thread_name)
            return

        if raw:
            try:
                events = spec.parse_message(raw)
            except Exception:
                log.exception("[%s] parse_message failed — dropping frame", thread_name)
                events = []
            if events:
                runtime.buffer.extend(events)

        now = clock()

        if runtime.buffer and (len(runtime.buffer) >= WS_BATCH_MAX or now - t_flush >= WS_BATCH_INTERVAL_S):
            _flush(runtime, spec, dsn, thread_name)
            t_flush = now

        if spec.ping_interval_s and now - t_ping >= spec.ping_interval_s:
            if spec.ping_frame is not None:
                ws.send(spec.ping_frame)
            t_ping = now

        if now - t_heartbeat >= WS_HEARTBEAT_INTERVAL_S:
            _heartbeat(runtime, config, spec, dsn, thread_name)
            t_heartbeat = now

        if now - t_resub >= WS_RESUB_CHECK_INTERVAL_S:
            topics = _resubscribe(ws, runtime, spec, config, dsn, topics, thread_name)
            t_resub = now

        if spec.proactive_reconnect_s and now - conn_start >= spec.proactive_reconnect_s:
            log.info("[%s] Proactive reconnect after %.0fs", thread_name, now - conn_start)
            return


def ws_loop(spec: WSFeedSpec, config: ExchangeConfig, dsn: str, *,
            connect_fn: Callable = create_connection, clock: Callable[[], float] = time.monotonic,
            max_sessions: int | None = None) -> None:
    """WS collector thread target (daemon). Owns the DB conn + event buffer across reconnects.

    A clean session end (graceful close / proactive reconnect) reconnects immediately (error_count
    was reset once established); a drop or connect failure increments error_count and backs off.
    `max_sessions` bounds the outer loop for tests (None = run forever).
    """
    thread_name = f"{config.name}/{spec.name}"
    runtime = _WSRuntime(conn=None, buffer=[])
    sessions = 0

    while max_sessions is None or sessions < max_sessions:
        sessions += 1
        try:
            ws = connect_fn(spec.url, timeout=WS_CONNECT_TIMEOUT_S)
        except WS_RETRIABLE as exc:
            runtime.error_count += 1
            log.warning("[%s] Connect failed (error_count=%d): %s", thread_name, runtime.error_count, exc)
            backoff_sleep(runtime.error_count)
            continue

        try:
            _run_session(ws, runtime, spec, config, dsn, thread_name, clock)
        except WS_RETRIABLE as exc:
            runtime.error_count += 1
            log.warning("[%s] Session dropped (error_count=%d): %s", thread_name, runtime.error_count, exc)
        finally:
            _flush(runtime, spec, dsn, thread_name)  # tail-flush before releasing the socket
            _safe_close(ws)

        if runtime.error_count:
            backoff_sleep(runtime.error_count)
