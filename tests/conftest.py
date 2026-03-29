"""Stub Docker-only deps and circular-import modules so collector imports on host."""

import sys
from unittest.mock import MagicMock

# External packages only available in Docker
for mod in ["psycopg", "psycopg.rows", "psycopg_pool", "dotenv"]:
    if mod not in sys.modules:
        sys.modules[mod] = MagicMock()

# Internal modules that cause circular imports (main ↔ funding ↔ oi)
# Stub before any collector module loads so `from main import connect_db` etc. resolve to mocks
for mod in ["main", "oi", "status", "backoff"]:
    if mod not in sys.modules:
        sys.modules[mod] = MagicMock()
