"""Tests for compression/retention migration — pattern-level validation."""

import pathlib

MIGRATION = pathlib.Path(__file__).parent.parent / "db" / "migrations" / "004-compression-retention.sql"
INIT_SQL = pathlib.Path(__file__).parent.parent / "db" / "init.sql"

TABLES_WITH_COMPRESSION = ["candles", "funding_rates", "open_interest"]
TABLES_WITH_RETENTION = ["candles", "funding_rates", "open_interest"]


def _read(path):
    return path.read_text()


def test_migration_file_exists():
    assert MIGRATION.exists(), "004-compression-retention.sql not found"


def test_compression_policies_for_all_tables():
    sql = _read(MIGRATION)
    for table in TABLES_WITH_COMPRESSION:
        assert f"add_compression_policy('{table}'" in sql, f"Missing compression policy for {table}"


def test_retention_policies_for_all_tables():
    sql = _read(MIGRATION)
    for table in TABLES_WITH_RETENTION:
        assert f"add_retention_policy('{table}'" in sql, f"Missing retention policy for {table}"


def test_candles_segmentby_includes_market_type():
    """Candles PK includes market_type — segmentby must too."""
    sql = _read(MIGRATION)
    # Find the ALTER TABLE candles block
    candles_idx = sql.index("ALTER TABLE candles")
    # Look for segmentby in the next ~200 chars
    segment = sql[candles_idx:candles_idx + 300]
    assert "market_type" in segment, "candles segmentby must include market_type (part of PK)"


def test_init_sql_has_compression_policies():
    """New installs (init.sql) should also get compression + retention."""
    sql = _read(INIT_SQL)
    for table in TABLES_WITH_COMPRESSION:
        assert f"add_compression_policy('{table}'" in sql, f"init.sql missing compression policy for {table}"


def test_init_sql_has_retention_policies():
    sql = _read(INIT_SQL)
    for table in TABLES_WITH_RETENTION:
        assert f"add_retention_policy('{table}'" in sql, f"init.sql missing retention policy for {table}"


# --- hl_asset_ctxs (migration 005) — HyperLiquid context bundle table --------

MIGRATION_005 = pathlib.Path(__file__).parent.parent / "db" / "migrations" / "005-hl-asset-ctxs.sql"

HL_CTX_CLAUSES = [
    "CREATE TABLE IF NOT EXISTS hl_asset_ctxs",
    "create_hypertable('hl_asset_ctxs'",
    "chunk_time_interval => INTERVAL '1 week'",
    "idx_hl_asset_ctxs_asset_ts",
    "add_compression_policy('hl_asset_ctxs'",
    "add_retention_policy('hl_asset_ctxs'",
]

# The 10 seed floats + keys, in the backtester seed's column order.
HL_CTX_COLUMNS = [
    "funding", "open_interest", "prev_day_px", "day_ntl_vlm", "premium",
    "oracle_px", "mark_px", "mid_px", "impact_bid_px", "impact_ask_px",
]


def test_migration_005_exists():
    assert MIGRATION_005.exists(), "005-hl-asset-ctxs.sql not found"


def test_migration_005_has_all_clauses():
    sql = _read(MIGRATION_005)
    for clause in HL_CTX_CLAUSES:
        assert clause in sql, f"005 missing: {clause}"


def test_init_sql_has_hl_asset_ctxs():
    """Fresh installs (init.sql) must also create the table with all clauses."""
    sql = _read(INIT_SQL)
    for clause in HL_CTX_CLAUSES:
        assert clause in sql, f"init.sql missing hl_asset_ctxs clause: {clause}"


def test_hl_asset_ctxs_columns_match_seed_order():
    """Column names/order must match the backtester Grade-B seed (hl-asset-ctxs.parquet)."""
    for path in (MIGRATION_005, INIT_SQL):
        sql = _read(path)
        idx = sql.index("CREATE TABLE IF NOT EXISTS hl_asset_ctxs")
        block = sql[idx:idx + 800]
        positions = [block.find(col) for col in HL_CTX_COLUMNS]
        assert all(p > 0 for p in positions), f"{path.name}: missing a seed column"
        assert positions == sorted(positions), f"{path.name}: seed columns out of order"
