"""Exponential backoff for collector error recovery.

Steps: 5s → 10s → 30s → 60s → 5m (per spec failure modes).
Resets on success. Catches specific ccxt and psycopg exceptions.
"""

import logging
import time

import ccxt
import psycopg

log = logging.getLogger(__name__)

BACKOFF_STEPS = [5, 10, 30, 60, 300]

# Exceptions that trigger backoff (transient / recoverable)
RETRIABLE_EXCEPTIONS = (
    ccxt.RateLimitExceeded,
    ccxt.NetworkError,
    ccxt.ExchangeNotAvailable,
    psycopg.OperationalError,
)


def backoff_sleep(error_count: int) -> None:
    """Sleep for the appropriate backoff duration based on consecutive error count."""
    idx = min(error_count, len(BACKOFF_STEPS)) - 1
    if idx < 0:
        return
    delay = BACKOFF_STEPS[idx]
    log.warning("Backoff: sleeping %ds (error_count=%d)", delay, error_count)
    time.sleep(delay)


def is_retriable(exc: Exception) -> bool:
    """Check if an exception should trigger backoff rather than immediate crash."""
    return isinstance(exc, RETRIABLE_EXCEPTIONS)
