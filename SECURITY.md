# Security

## Reporting a vulnerability

Report it privately with GitHub's **Report a vulnerability** button on this repo's Security tab. Please don't open a public issue. Expect a first reply within a week.

Only the latest release is supported.

## Design boundaries

These are deliberate, not vulnerabilities:

- The API has no authentication. It's read-only and meant to stay off the public internet: the production compose file binds it to `127.0.0.1`, and you reach it over an SSH tunnel.
- The development compose file publishes Postgres (5432) and the API (8000) on every interface. It's for laptops, not servers.
