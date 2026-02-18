# Trino Tuner (trino-tuner)

A small, production-minded MVP that takes a Trino SQL query, runs **EXPLAIN** to understand the plan, fetches relevant **table metadata**, and uses an **LLM** to propose an optimized rewrite—then validates the rewrite by re-running EXPLAIN and (optionally) basic safety checks.

> Goal: faster, safer, more cost-efficient Trino queries—without breaking correctness.

---

## What it does

1. **Accepts a Trino SQL query**
2. Runs **EXPLAIN (FORMAT JSON)** (or standard EXPLAIN if JSON is unavailable)
3. **Parses SQL** to extract referenced tables
4. Fetches **table metadata** (columns, types; optionally partitions when available)
5. Builds a **grounded optimization prompt** (query + plan + schema + rules)
6. Asks an **LLM** to rewrite the query using best practices
7. Re-runs **EXPLAIN** on the rewritten query
8. If the rewrite fails (syntax/validation), it **auto-fixes once**
9. Returns:
   - optimized SQL
   - warnings
   - before/after plan summary (lightweight)

---

## Key optimization rules (built-in)

- **Partition pruning**: If filtering by timestamp but table has `dt/ds/event_date`, add a matching partition predicate.
- **LIMIT hygiene**: keep LIMIT; add a default exploratory LIMIT (e.g. 500) when appropriate.
- **Avoid `SELECT *`** when safe (warn if risky).
- **CTEs** to avoid repeating predicates or duplicated subqueries.
- **Join hygiene**: push filters/projections early; encourage small build side; avoid Cartesian joins.
- Prefer **`UNION ALL`** when dedupe is not required (warn on `UNION`).
- Use **approx** functions (`approx_distinct`, etc.) when acceptable (warn when rewriting exact).
- Keep **correctness as highest priority**.

---

## Repo structure

```text
trino-tuner/
  src/
    trino_tuner/
      api.py              # FastAPI web service
      streamlit_app.py    # Local UI
      trino_client.py     # Trino JDBC/HTTP execution helpers
      sql_parse.py        # Table extraction from SQL
      metadata.py         # Schema/partition metadata fetch
      prompt.py           # LLM prompt builder
      optimizer.py        # Orchestration: explain -> prompt -> rewrite -> validate
      rules.json          # Optimization guidance rules (JSON)
  examples/
    sample_queries.sql
  policy/
    safety.yaml           # Optional: read-only, deny schemas, etc.
  .env.example
  requirements.txt
  README.md

  ```

## Requirements
	•	Python 3.10+
	•	Access to a Trino coordinator (local Docker or remote)
	•	One LLM option:
	•	OpenAI API key (default), or
	•	Any compatible hosted/local model you wire in

## Install
```
git clone git@github.com:<your-org>/trino-tuner.git
cd trino-tuner

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
cp .env.example .env
```

## Configure

Edit .env:
```
# ---- Trino connection ----
TRINO_HOST=localhost
TRINO_PORT=8080
TRINO_USER=trino
TRINO_CATALOG=postgres
TRINO_SCHEMA=public
TRINO_HTTP_SCHEME=http

# Optional (if your Trino requires auth)
# TRINO_PASSWORD=...
# TRINO_SSL=true

# ---- LLM ----
OPENAI_API_KEY=your_key_here
OPENAI_MODEL=gpt-4o-mini   # example; choose your model
LLM_TEMPERATURE=0

# ---- Tuning behavior ----
DEFAULT_EXPLORATORY_LIMIT=500
MAX_FIX_ATTEMPTS=1
RETURN_PLAN_SUMMARY=true
```


### Create an OpenAI API key

Go to:
	•	https://platform.openai.com/apps-manage

Create an API key and paste it into OPENAI_API_KEY in your .env.

## Run: FastAPI service
```
python -m src.trino_tuner.api
```

By default the API serves on:
	•	http://127.0.0.1:8000

### Endpoints

#### POST /optimize
##### Request:
```
{
  "sql": "SELECT ...",
  "catalog": "postgres",
  "schema": "public",
  "explain_only": true
}
```


##### Response (example):
```
{
  "optimized_sql": "SELECT ...",
  "warnings": ["UNION used; consider UNION ALL if dedupe not required"],
  "before": { "plan_summary": "..." },
  "after": { "plan_summary": "..." }
}
```

### Try with curl
```
curl -X POST http://127.0.0.1:8000/optimize \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM postgres.public.\"Artist\" LIMIT 10","catalog":"postgres","schema":"public","explain_only":true}'
```

### Run: Streamlit UI (local)

```
streamlit run src/trino_tuner/streamlit_app.py
```

What you get:
	•	SQL input box
	•	“Optimize” button
	•	Results: optimized SQL + warnings + (optional) plan summary


## How Trino connection works

### Trino Tuner submits:
	•	EXPLAIN (FORMAT JSON) <your_query> (preferred)
	•	falls back to EXPLAIN <your_query> if JSON format isn’t supported

### Metadata is fetched using:
	•	DESCRIBE <catalog>.<schema>.<table>
	•	optional: connector-specific partition discovery (future improvement)

⸻

### Safety / correctness
	•	Designed to be read-only by default
	•	You can add guardrails (deny schemas/tables, enforce LIMIT, require tenant filters) in policy/safety.yaml (optional)
	•	The tool never runs DDL/DML unless you explicitly change policy.



## Privacy
	•	Query text + explain plan + selected metadata are sent to the LLM provider if using a hosted model.
	•	To keep everything local, wire a local model backend and disable hosted LLM usage.

⸻

## Roadmap
	•	Connector-aware partition metadata (Iceberg/Hive/Delta/BigQuery/etc.)
	•	Better plan diff (stage-level stats, scanned bytes, CPU time)
	•	Cost heuristics and “why this is faster” explanations
	•	Multi-tenant safety enforcement (required_filters)
	•	Caching schema/metadata for speed
	•	Batch mode for tuning many queries

⸻

## Troubleshooting

“Catalog must be specified”

Your session catalog is not set. Use fully qualified names:

```
SELECT * FROM postgres.public.my_table;
```


Or configure TRINO_CATALOG / TRINO_SCHEMA in .env.

Trino can’t see Postgres tables

Verify Trino catalog file (example: /etc/trino/catalog/postgres.properties):

```
connector.name=postgresql
connection-url=jdbc:postgresql://postgres:5432/exampledb
connection-user=trinouser
connection-password=trinopassword
```

Then:

```
SHOW CATALOGS;
SHOW SCHEMAS FROM postgres;
SHOW TABLES FROM postgres.public;
```


## License

MIT license

## Disclaimer

Trino Tuner provides suggestions. Always validate correctness and performance in your environment (especially for compliance/billing-critical workloads).