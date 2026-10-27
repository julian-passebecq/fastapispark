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

## Real Spark verification

SparkLab remains the default interactive path. It uses DuckDB for bounded real
results and a deterministic model for distributed Spark concepts.

For an explicit oracle run, Datapass can dispatch the same educational source
and bounded fixtures to GitHub Actions, where Apache Spark **4.2.0** runs in
`local[4]` mode on one GitHub-hosted Ubuntu VM.

This is genuine Spark execution, but it is **single-host**, not a multi-machine
cluster.

Remote endpoints:

- `GET /v1/spark/verify/capabilities`
- `POST /v1/spark/verify`
- `GET /v1/spark/verify/{run_id}`
- `GET /v1/spark/verify/{run_id}/result/{request_id}`

The verification artifact includes bounded result rows, Spark logical and
physical plans, formatted explain output, the Spark event log, and measured
task/stage/shuffle/spill summaries.

The FastAPI server needs a server-side GitHub token with Actions write/read
permission for the execution repository:

```text
DATAPASS_GITHUB_TOKEN=<server-only token>
DATAPASS_RUNNER_KEY=<server-to-server secret>
DATAPASS_SPARK_GITHUB_REPO=julian-passebecq/fastapispark
DATAPASS_SPARK_GITHUB_WORKFLOW=real-spark.yml
DATAPASS_SPARK_GITHUB_REF=main
DATAPASS_SPARK_PUBLIC_REPO=1
```

Do not expose either secret to the browser. The verify/status/result endpoints require the `X-Datapass-Runner-Key` header; the Datapass backend should add it server-side when proxying to this service. On a public execution repository, submitted source, logs and artifacts must contain no secrets or private data.

## Runtime profiles

The API exposes four fictional Datapass profiles: `datapass-free`, `datapass-s`, `datapass-m`, and `datapass-l`. These are educational parameters, not Microsoft Fabric, Databricks or cloud-provider prices.

## Safety boundaries

- The normal FastAPI/DuckDB SparkLab path never `exec()`s or `eval()`s notebook Python; it parses a bounded PySpark-like subset.
- The optional **real Spark oracle** deliberately executes submitted educational PySpark inside an ephemeral GitHub-hosted VM. That runner is a separate trust boundary: do not submit credentials, private company code or personal/customer data.
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

### Real Spark runner contract

The public API now exposes provider-neutral job identifiers in addition to the
legacy GitHub Actions run id. A dispatch returns both:

```json
{"job_id":"github:123456789","run_id":123456789}
```

Poll the stable runner contract with:

```text
GET /v1/spark/jobs/{job_id}
GET /v1/spark/jobs/{job_id}/result/{request_id}
```

Today `github:` jobs execute on GitHub Actions. The identifier and endpoint
shape deliberately do not force Datapass clients to know that implementation,
so a persistent Oracle A1 Spark backend can be added later without replacing
the notebook/UI contract. The legacy `/v1/spark/verify/{run_id}` routes remain
available during migration.

```bash
uv run fastapi cloud deploy . --json
```

For Datapass web integration, set `DATAPASS_CORS_ORIGINS` to a comma-separated allowlist of frontend origins. The default is `*` for development.
