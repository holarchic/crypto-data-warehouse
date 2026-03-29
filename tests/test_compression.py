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
