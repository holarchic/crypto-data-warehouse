"""scripts/export-feed.py against a local HTTP stand-in for /export/parquet: real urllib, real pyarrow."""

import importlib.util
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

_SPEC = importlib.util.spec_from_file_location(
    "export_feed", Path(__file__).resolve().parents[1] / "scripts" / "export-feed.py")
export_feed = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(export_feed)

T0 = datetime(2026, 7, 1, tzinfo=timezone.utc)
HOUR = timedelta(hours=1)


def _parquet(start: datetime, n: int, *, wide: bool = False) -> bytes:
    cols = {"coin": ["BTC"] * n, "time_ms": [int(start.timestamp()) * 1000 + i for i in range(n)]}
    if wide:
        cols["extra"] = [1.0] * n
    buf = BytesIO()
    pq.write_table(pa.table(cols), buf)
    return buf.getvalue()


class _Api:
    """Serves /export/parquet from `respond(start, end) -> (status, body)` and records each window asked."""

    def __init__(self, respond):
        self.respond = respond
        self.windows = []
        api = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                q = {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}
                start, end = datetime.fromisoformat(q["start"]), datetime.fromisoformat(q["end"])
                api.windows.append((start, end, q.get("asset_id")))
                status, body = api.respond(start, end)
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def serve():
    servers = []

    def start(respond):
        servers.append(_Api(respond))
        return servers[-1]

    yield start
    for s in servers:
        s.close()


def _export(api, out, end, **kw):
    return export_feed.export_span(api.url, "hl_asset_ctxs", T0, end, out, log=lambda _: None, **kw)


def test_windows_stream_into_one_file_one_row_group_each(serve, tmp_path):
    api = serve(lambda s, e: (200, _parquet(s, 3)))
    out = tmp_path / "f.parquet"
    assert _export(api, out, T0 + 3 * HOUR, step=HOUR) == 9
    f = pq.ParquetFile(out)
    assert f.metadata.num_row_groups == 3  # written per window, not accumulated then written
    assert f.read().column("time_ms").to_pylist() == [
        int((T0 + h * HOUR).timestamp()) * 1000 + i for h in range(3) for i in range(3)]
    assert [(s, e) for s, e, _ in api.windows] == [(T0 + h * HOUR, T0 + (h + 1) * HOUR) for h in range(3)]
    assert not (tmp_path / "f.parquet.partial").exists()


def test_413_halves_the_window_and_keeps_the_smaller_step(serve, tmp_path):
    api = serve(lambda s, e: (413, b"too large") if e - s > 6 * HOUR else (200, _parquet(s, 1)))
    out = tmp_path / "f.parquet"
    assert _export(api, out, T0 + 12 * HOUR, step=24 * HOUR) == 2
    # the 24 h step is clipped to the 12 h span (413), halves to 12 h (413), then 6 h, kept for the next window
    assert [e - s for s, e, _ in api.windows] == [12 * HOUR, 12 * HOUR, 6 * HOUR, 6 * HOUR]
    assert [s for s, e, _ in api.windows if e - s == 6 * HOUR] == [T0, T0 + 6 * HOUR]


def test_out_appears_only_after_the_last_window(serve, tmp_path):
    out = tmp_path / "f.parquet"
    seen = []

    def respond(s, e):
        seen.append((out.exists(), (tmp_path / "f.parquet.partial").exists()))
        return 200, _parquet(s, 1)

    _export(serve(respond), out, T0 + 3 * HOUR, step=HOUR)
    assert seen == [(False, False), (False, True), (False, True)]
    assert out.exists()


def test_204_windows_are_skipped(serve, tmp_path):
    api = serve(lambda s, e: (204, b"") if s == T0 else (200, _parquet(s, 2)))
    out = tmp_path / "f.parquet"
    assert _export(api, out, T0 + 2 * HOUR, step=HOUR) == 2
    assert len(api.windows) == 2


def test_asset_id_and_last_window_clipped_to_end(serve, tmp_path):
    api = serve(lambda s, e: (200, _parquet(s, 1)))
    _export(api, tmp_path / "f.parquet", T0 + 90 * timedelta(minutes=1), step=HOUR, asset_id="BTC")
    assert api.windows == [(T0, T0 + HOUR, "BTC"), (T0 + HOUR, T0 + 90 * timedelta(minutes=1), "BTC")]


@pytest.mark.parametrize("respond, message", [
    (lambda s, e: (200, _parquet(s, 1, wide=s > T0)), "schema changed"),
    (lambda s, e: (500, b"boom") if s > T0 else (200, _parquet(s, 1)), "HTTP 500"),
    (lambda s, e: (204, b""), "no rows"),
    (lambda s, e: (413, b""), "still exceeds the row cap"),
])
def test_a_failed_run_leaves_no_output_and_no_partial(serve, tmp_path, respond, message):
    api = serve(respond)
    out = tmp_path / "f.parquet"
    with pytest.raises(export_feed.ExportError, match=message):
        _export(api, out, T0 + 2 * HOUR, step=HOUR)
    assert list(tmp_path.iterdir()) == []


def test_a_stalled_request_times_out_without_output(serve, tmp_path):
    def slow(s, e):
        if s > T0:
            time.sleep(1)
        return 200, _parquet(s, 1)

    api = serve(slow)
    with pytest.raises(OSError):
        _export(api, tmp_path / "f.parquet", T0 + 2 * HOUR, step=HOUR, timeout=0.2)
    assert list(tmp_path.iterdir()) == []


def test_cli_rejects_naive_times_and_reports_failure(serve, tmp_path, capsys):
    with pytest.raises(SystemExit):
        export_feed.main(["--api", "x", "--table", "t", "--start", "2026-07-01T00:00", "--end",
                          "2026-07-02T00:00:00Z", "--out", str(tmp_path / "f.parquet")])
    api = serve(lambda s, e: (204, b""))
    assert export_feed.main(["--api", api.url, "--table", "t", "--start", "2026-07-01T00:00:00Z", "--end",
                             "2026-07-01T02:00:00Z", "--out", str(tmp_path / "f.parquet")]) == 1
    assert "no rows" in capsys.readouterr().err
