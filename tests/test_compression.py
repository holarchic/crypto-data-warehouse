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


# --- open_interest chunk interval → 1 week (migration 007) -------------------
# Densified OI multiplies row volume; init.sql + migration 007 must agree, so guard
# both against drift at the text level (codex-LOW, T-007.2).

MIGRATION_007 = pathlib.Path(__file__).parent.parent / "db" / "migrations" / "007-oi-chunk-interval.sql"


def test_migration_007_sets_open_interest_chunk_to_one_week():
    assert MIGRATION_007.exists(), "007-oi-chunk-interval.sql not found"
    sql = _read(MIGRATION_007)
    assert "set_chunk_time_interval('open_interest', INTERVAL '1 week')" in sql


def test_init_sql_open_interest_chunk_is_one_week():
    """Fresh installs must create open_interest with the 1-week chunk interval."""
    sql = _read(INIT_SQL)
    idx = sql.index("create_hypertable('open_interest'")
    block = sql[idx:idx + 200]
    assert "chunk_time_interval => INTERVAL '1 week'" in block, "init.sql open_interest not 1-week chunks"


# --- liquidations (migration 008) — all-venue liquidation events ------------
# Empirically validated on timescaledb pg16 (dedup + compression). Guard migration ≡
# init.sql at the text level so the two never drift (T-009.4).

MIGRATION_008 = pathlib.Path(__file__).parent.parent / "db" / "migrations" / "008-liquidations.sql"

LIQ_CLAUSES = [
    "CREATE TABLE IF NOT EXISTS liquidations",
    "create_hypertable('liquidations'",
    "chunk_time_interval => INTERVAL '1 week'",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_liquidations_dedup",
    "timescaledb.compress_segmentby = 'venue, asset_id'",
    "timescaledb.compress_orderby = 'timestamp DESC'",
    "add_compression_policy('liquidations'",
    "add_retention_policy('liquidations'",
]

# The HL seed's 13 columns + venue/grade/liquidated_side, in DDL order.
LIQ_COLUMNS = [
    "timestamp", "venue", "grade", "asset_id", "liquidated_side", "side", "dir",
    "px", "sz", "mark_px", "closed_pnl", "method", "liquidated_user", "oid", "tid", "day",
]


def test_migration_008_exists():
    assert MIGRATION_008.exists(), "008-liquidations.sql not found"


def test_migration_008_has_all_clauses():
    sql = _read(MIGRATION_008)
    for clause in LIQ_CLAUSES:
        assert clause in sql, f"008 missing: {clause}"


def test_init_sql_has_liquidations():
    """Fresh installs (init.sql) must create liquidations with every clause the migration has."""
    sql = _read(INIT_SQL)
    for clause in LIQ_CLAUSES:
        assert clause in sql, f"init.sql missing liquidations clause: {clause}"


def _liq_column_defs(path):
    """Extract [(name, normalized type+constraints), ...] from the CREATE TABLE liquidations block."""
    sql = _read(path)
    start = sql.index("(", sql.index("CREATE TABLE IF NOT EXISTS liquidations")) + 1
    body = sql[start:sql.index(");", start)]
    defs = []
    for line in body.splitlines():
        line = line.strip().rstrip(",").strip()
        if not line:
            continue
        name, _, rest = line.partition(" ")
        defs.append((name, " ".join(rest.split()).upper()))
    return defs


def test_liquidations_columns_are_seed_order():
    """Each file lists the 16 columns (seed 13 + venue/grade/liquidated_side) in DDL order."""
    for path in (MIGRATION_008, INIT_SQL):
        names = [name for name, _ in _liq_column_defs(path)]
        assert names == LIQ_COLUMNS, f"{path.name}: liquidations columns drifted from seed order"


def test_liquidations_migration_and_init_are_identical():
    """migration 008 ≡ init.sql at the column level — name, type, AND nullability, no extras.
    A weaker per-file check would miss a type/NOT-NULL/extra-column drift between the two files."""
    assert _liq_column_defs(MIGRATION_008) == _liq_column_defs(INIT_SQL)


def test_liquidations_dedup_index_preserves_oid_tid():
    """The dedup key MUST be (venue, asset_id, timestamp, oid, tid). Dropping oid/tid would
    collapse the HL tape's real same-ms clustered events (load-bearing claim). Pin it in both files."""
    for path in (MIGRATION_008, INIT_SQL):
        sql = _read(path)
        idx = sql.index("idx_liquidations_dedup")
        block = sql[idx:idx + 120]
        assert "(venue, asset_id, timestamp, oid, tid)" in block, f"{path.name}: dedup index shape drifted"


# --- premium_index (migration 009) — Binance premium-index term structure ----
# Guard migration ≡ init.sql at the text level so the two never drift (T-010).

MIGRATION_009 = pathlib.Path(__file__).parent.parent / "db" / "migrations" / "009-premium-index.sql"

PREMIUM_CLAUSES = [
    "CREATE TABLE IF NOT EXISTS premium_index",
    "create_hypertable('premium_index'",
    "chunk_time_interval => INTERVAL '1 week'",
    "idx_premium_index_asset_ts",
    "timescaledb.compress_segmentby = 'exchange, symbol'",
    "timescaledb.compress_orderby = 'timestamp DESC'",
    "add_compression_policy('premium_index'",
    "add_retention_policy('premium_index'",
]

# Identity cols + premium OHLC, in DDL order (native_symbol = seed `coin`).
PREMIUM_COLUMNS = [
    "exchange", "asset_id", "symbol", "native_symbol", "timestamp",
    "premium_open", "premium_high", "premium_low", "premium_close",
]


def _premium_column_defs(path):
    """[(name, normalized type+constraints)] from the CREATE TABLE premium_index block,
    skipping the trailing PRIMARY KEY line (a constraint, not a column)."""
    sql = _read(path)
    start = sql.index("(", sql.index("CREATE TABLE IF NOT EXISTS premium_index")) + 1
    body = sql[start:sql.index(");", start)]
    defs = []
    for line in body.splitlines():
        line = line.strip().rstrip(",").strip()
        if not line or line.upper().startswith("PRIMARY KEY"):
            continue
        name, _, rest = line.partition(" ")
        defs.append((name, " ".join(rest.split()).upper()))
    return defs


def test_migration_009_exists():
    assert MIGRATION_009.exists(), "009-premium-index.sql not found"


def test_migration_009_has_all_clauses():
    sql = _read(MIGRATION_009)
    for clause in PREMIUM_CLAUSES:
        assert clause in sql, f"009 missing: {clause}"


def test_init_sql_has_premium_index():
    """Fresh installs (init.sql) must create premium_index with every clause the migration has."""
    sql = _read(INIT_SQL)
    for clause in PREMIUM_CLAUSES:
        assert clause in sql, f"init.sql missing premium_index clause: {clause}"


def test_premium_index_columns_are_ddl_order():
    for path in (MIGRATION_009, INIT_SQL):
        names = [name for name, _ in _premium_column_defs(path)]
        assert names == PREMIUM_COLUMNS, f"{path.name}: premium_index columns drifted"


def test_premium_index_migration_and_init_are_identical():
    """migration 009 ≡ init.sql at the column level — name, type, AND nullability, no extras."""
    assert _premium_column_defs(MIGRATION_009) == _premium_column_defs(INIT_SQL)


def test_premium_index_pk_shape():
    """PK MUST be (exchange, symbol, timestamp) — the hypertable partition col (timestamp)
    must be in the key, and per-symbol idempotency needs symbol. Pin it in both files."""
    for path in (MIGRATION_009, INIT_SQL):
        sql = _read(path)
        idx = sql.index("CREATE TABLE IF NOT EXISTS premium_index")
        block = sql[idx:idx + 600]
        assert "PRIMARY KEY (exchange, symbol, timestamp)" in block, f"{path.name}: premium_index PK shape drifted"
