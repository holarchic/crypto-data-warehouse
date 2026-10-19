"""Tests for collector/slots.py — fixed-rate slot pacing shared by every REST poll loop."""

import logging

import pytest
import slots


@pytest.fixture
def clock(monkeypatch):
    """Fake wall clock: `now` is settable, sleeps are recorded and advance it."""
    state = {"now": 0.0, "slept": []}

    def sleep(s):
        state["slept"].append(s)
        state["now"] += s

    monkeypatch.setattr(slots.time, "time", lambda: state["now"])
    monkeypatch.setattr(slots.time, "sleep", sleep)
    return state


# --- next_slot ------------------------------------------------------------------


def test_next_slot_from_a_slot_is_the_following_slot():
    assert slots.next_slot(330, 300, 30) == 630


def test_next_slot_between_boundary_and_buffer_point_is_that_periods_slot():
    # A cycle ending at 610 is past the 600 boundary but before the 630 slot: 630, not 930.
    assert slots.next_slot(610, 300, 30) == 630


def test_next_slot_past_the_slot_is_the_following_one():
    assert slots.next_slot(640, 300, 30) == 930


def test_next_slot_clamps_buffer_to_half_interval():
    # A 30 s buffer on a 30 s interval would fold the slot onto the boundary; it clamps to 15.
    assert slots.next_slot(1000, 30, 30) == 1005


# --- sleep_until_next_slot ------------------------------------------------------


def test_sleeps_to_the_slot_after_the_cycle_start(clock):
    clock["now"] = 400.0  # cycle started on its 330 slot and ran 70 s
    slots.sleep_until_next_slot(300, 30, 330.0, "binance/5m")
    assert clock["slept"] == [230.0]


def test_overrun_before_the_buffer_point_waits_for_that_slot(clock):
    # Dense OI: started on slot 990, ran 37 s to 1027, past the 1020 boundary. Waits 23 s for 1050.
    clock["now"] = 1027.0
    slots.sleep_until_next_slot(60, 30, 990.0, "binance/oi")
    assert clock["slept"] == [23.0]


def test_overrun_past_the_slot_starts_at_once(clock, caplog):
    # Started on 330, ended at 680, past its 630 slot: no sleep (the old rules waited until 930).
    clock["now"] = 680.0
    with caplog.at_level(logging.INFO, logger="slots"):
        slots.sleep_until_next_slot(300, 30, 330.0, "hyperliquid/5m")
    assert clock["slept"] == []
    assert "[hyperliquid/5m]" in caplog.text
    assert "slot passed 50s ago, starting now" in caplog.text


def test_no_previous_cycle_waits_for_the_slot_after_now(clock, caplog):
    clock["now"] = 400.0
    with caplog.at_level(logging.INFO, logger="slots"):
        slots.sleep_until_next_slot(300, 30, None, "binance/premium")
    assert clock["slept"] == [230.0]
    # The line carries the time the target was taken from, so a check can recompute it.
    assert "[binance/premium] no previous cycle, now 1970-01-01T00:06:40.000+00:00; next slot" in caplog.text


def test_cycle_end_line_records_start_and_target(clock, caplog):
    clock["now"] = 400.0
    with caplog.at_level(logging.INFO, logger="slots"):
        slots.sleep_until_next_slot(300, 30, 330.0, "binance/5m")
    assert ("[binance/5m] Cycle started 1970-01-01T00:05:30.000+00:00; "
            "next slot 1970-01-01T00:10:30.000+00:00; sleeping 230s") in caplog.text


def test_late_runs_follow_back_to_back_then_realign(clock):
    # I 300, b 30: a cycle starting at 330 runs 350 s, then 280 s cycles. Starts: 680, 960,
    # 1240 back to back (each ended past its slot), then 1530 on the grid once 1240 + 280 = 1520
    # ends before its 1530 slot.
    starts = []
    started = None
    clock["now"] = 330.0
    for duration in (350, 280, 280, 280):
        if started is not None:
            slots.sleep_until_next_slot(300, 30, started, "hyperliquid/5m")
        started = clock["now"]
        starts.append(started)
        clock["now"] += duration
    slots.sleep_until_next_slot(300, 30, started, "hyperliquid/5m")
    starts.append(clock["now"])
    assert starts == [330.0, 680.0, 960.0, 1240.0, 1530.0]
