"""Tests for collector/discovery.py — union qualification with hysteresis."""

import importlib.util
import re
from pathlib import Path

from discovery import qualify_assets

ENTRY = 1_000_000
KEEP = 500_000


def test_new_asset_enters_at_threshold():
    assert qualify_assets({"A": ENTRY}, set(), ENTRY, KEEP) == {"A"}


def test_inactive_asset_between_keep_and_entry_stays_out():
    assert qualify_assets({"A": 900_000}, set(), ENTRY, KEEP) == set()


def test_active_asset_between_keep_and_entry_is_kept():
    assert qualify_assets({"A": 600_000}, {"A"}, ENTRY, KEEP) == {"A"}


def test_active_asset_below_keep_is_dropped():
    assert qualify_assets({"A": 400_000}, {"A"}, ENTRY, KEEP) == set()


def test_keep_equal_to_entry_is_a_single_threshold():
    volumes = {"active_dip": 900_000, "active_high": 1_200_000, "new_dip": 900_000}
    assert qualify_assets(volumes, {"active_dip", "active_high"}, ENTRY, ENTRY) == {"active_high"}


def _real_status_module():
    """conftest stubs `status` for host runs; load the real file (it imports only `logging`) to pin its SQL."""
    path = Path(__file__).resolve().parents[1] / "collector" / "status.py"
    spec = importlib.util.spec_from_file_location("status_real", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_collector_success_write_never_reactivates_a_pair():
    """Hysteresis reads `active`, so only discovery may set it: a collector cycle holding a pre-discovery
    pair list must not resurrect a pair discovery just dropped (it would then re-enter at the keep threshold)."""
    on_conflict = _real_status_module().UPSERT_SUCCESS_SQL.split("DO UPDATE SET", 1)[1]
    assert not re.search(r"\bactive\s*=", on_conflict)
