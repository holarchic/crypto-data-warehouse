"""Test doubles shared across test modules."""

from dataclasses import dataclass


@dataclass(frozen=True)
class DbEvent:
    """One call a RecordingConn saw: `kind` is execute, executemany, commit or rollback."""

    kind: str
    sql: str | None = None
    params: object = None


class RecordingConn:
    """A connection and its cursor in one: records every execute and transaction call as a DbEvent;
    fetchall answers `rows`."""

    rowcount = 0
    description = ()
    closed = False

    def __init__(self, rows=()):
        self.rows = list(rows)
        self.events: list[DbEvent] = []

    def kinds(self) -> list[str]:
        return [event.kind for event in self.events]

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.events.append(DbEvent("execute", sql, params))

    def executemany(self, sql, rows):
        self.events.append(DbEvent("executemany", sql, rows))

    def fetchall(self):
        return self.rows

    def commit(self):
        self.events.append(DbEvent("commit"))

    def rollback(self):
        self.events.append(DbEvent("rollback"))
