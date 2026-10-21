FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends libpq5 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --timeout 300 -r requirements.txt

# The release version. `project` is the repo root, a named build context: compose passes it
# (additional_contexts); a bare build needs `docker build --build-context project=. api`.
COPY --from=project pyproject.toml /pyproject.toml
# Feed metadata: the collector's config module and its exchanges.yaml, the one source of candle-table
# routing, timeframe intervals and loop names (main.py imports config). config.py must import only
# the stdlib and yaml.
COPY --from=project collector/config.py collector/exchanges.yaml /app/
COPY . .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
