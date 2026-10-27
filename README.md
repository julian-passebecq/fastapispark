# Datapass Fake Spark Runtime

`fastapispark` is the stateless backend used by **Datapass Studio / SparkLab** to provide a realistic Spark-learning experience without running an actual Spark cluster.

It deliberately separates two concerns:

1. **Truthful local execution** — bounded in-memory datasets are executed with DuckDB.
2. **Spark simulation** — a deterministic model produces Spark-like stages, tasks, partitions, shuffle I/O, spill pressure, skew effects, elapsed time and fictional Datapass compute credits.

The simulator never claims those distributed metrics are measurements from a real Spark cluster.

## Why this architecture

A free FastAPI service is not a multi-node Spark cluster. Datapass instead uses DuckDB for correct table results and a separate teaching model for distributed-system behavior.

This gives the React/Fluent notebook UI enough information to render a Fabric/Databricks-style execution experience while keeping the backend small, fast and deployable.

## API

- `GET /health` — health probe.
- `GET /v1/runtimes` — fictional Datapass cluster profiles.
- `POST /v1/spark/compile` — compile a safe PySpark-like subset into a structured Datapass plan.
- `POST /v1/spark/plan` — compile a structured plan to DuckDB SQL and simulate Spark metrics without executing data.
- `POST /v1/spark/execute` — execute a structured plan on bounded uploaded rows and return simulated Spark metrics.
- `POST /v1/spark/sql` — execute a single safe in-memory `SELECT`/`WITH` query.

Interactive OpenAPI documentation is at `/docs`.

## Runtime profiles

The API exposes four fictional Datapass profiles: `datapass-free`, `datapass-s`, `datapass-m`, and `datapass-l`. These are educational parameters, not Microsoft Fabric, Databricks or cloud-provider prices.

## Safety boundaries

- No `exec()` or `eval()` of notebook Python.
- SQL is restricted to one comment-free `SELECT`/`WITH` query over in-memory tables.
- DuckDB external access and automatic extension loading are disabled when supported.
- Input tables are bounded to 5,000 rows each and eight tables per request.
- Result collection is bounded to 500 rows.
- Simulation hints can model very large datasets without uploading them.
- The service is stateless; no notebook state, credentials or user datasets are persisted.

## Local development

Prerequisites: **Python 3.11 or later** and [uv](https://docs.astral.sh/uv/). Run the commands from the repository root. Dependencies are declared in [pyproject.toml](pyproject.toml); the first sync downloads them.

```bash
uv sync --dev
uv run pytest
uv run fastapi dev main.py
```

Open <http://127.0.0.1:8000/docs> for the interactive API. The development server keeps running until you press Ctrl+C; use a second terminal for the example below.

### Execute the included example

The [sample request](examples/datapass_execute.json) filters three synthetic sales rows, groups by region and sorts revenue descending. On macOS, Linux or Git Bash:

```bash
curl --fail-with-body --silent --show-error \
  -H 'Content-Type: application/json' \
  --data-binary @examples/datapass_execute.json \
  http://127.0.0.1:8000/v1/spark/execute
```

In PowerShell:

```powershell
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/v1/spark/execute' -ContentType 'application/json' -InFile 'examples/datapass_execute.json'
```

The response's `physical_engine` is `duckdb`. Its actual result rows are:

```json
[
  {"region": "CH", "revenue": 240.0},
  {"region": "NO", "revenue": 180.0}
]
```

The same request supplies a five-million-row **simulation hint**. It does not upload or execute five million rows; `metrics` contains the teaching model's estimates, while `rows` comes from the three supplied input rows.

### Inspect the implementation and checks

[main.py](main.py) contains request validation, plan compilation, bounded DuckDB execution and the simulation model. [The tests](tests/test_api.py) cover health, rejection of arbitrary Python, the supported compiler subset and simulated shuffle behavior. [The CI workflow](.github/workflows/ci.yml) runs those tests; [its recorded results](https://github.com/julian-passebecq/fastapispark/actions/workflows/ci.yml) apply to the named commits.

This is an educational prototype. The current test suite and input limits are not a complete untrusted-query security or production-load qualification.

## FastAPI Cloud

The repository is structured for FastAPI Cloud with a root `main.py` exposing `app` and a `pyproject.toml` declaring all runtime dependencies.

```bash
uv run fastapi cloud deploy . --json
```

For Datapass web integration, set `DATAPASS_CORS_ORIGINS` to a comma-separated allowlist of frontend origins. The default is `*` for development.
