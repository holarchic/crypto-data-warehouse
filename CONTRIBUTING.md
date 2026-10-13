# Contributing

Contributions are welcome: bug reports, fixes, new exchanges, Kubernetes manifests, docs.

For anything bigger than a small fix, open an issue first so we can agree the approach before you build it.

## Setup

You need Python 3.12+, [uv](https://github.com/astral-sh/uv) and Docker.

```bash
uv run --extra dev ruff check .
uv run --extra dev pytest -q        # no database or network needed
docker compose up -d                # the full stack, for end-to-end checks
```

## Conventions

- All timestamps are UTC, candle-open time.
- Parameterize all SQL. No string interpolation.
- Writes are idempotent: `INSERT … ON CONFLICT DO NOTHING`.
- Symbols use ccxt format; `asset_id` is the canonical base (`collector/symbols.py`).
- Functions over classes. Guard clauses and early returns.
- Data integrity over speed: never drop or invent data silently. A failure is logged and surfaces in `/status`.

## Schema changes

Add a numbered file to `db/migrations/` and make the same change in `db/init.sql`, which is all a fresh install runs.

## Adding an exchange

The exchange must be supported by [ccxt](https://github.com/ccxt/ccxt).

1. Add an entry to `collector/exchanges.yaml`: ccxt class, BTC symbol, market type, rate limit, supported timeframes, and the `has_funding` / `has_oi` flags.
2. Add a `collector-<name>` service to `docker-compose.yml`, and a memory limit for it in `docker-compose.prod.yml`.
3. If the exchange names assets unusually, add a rule to `collector/symbols.py`.
4. Add tests, and check it end to end: `GET /status` shows its rows fresh.

## Pull requests

- One concern per PR, with tests.
- `ruff check` and `pytest` pass.
- Say what changed and how you verified it.

## Bug reports

Include what you ran, what you expected, the relevant `docker compose logs`, and the output of `GET /status`.

## License

By contributing, you agree your contributions are licensed under the MIT License.
