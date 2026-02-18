from __future__ import annotations

import json
from typing import List

from .metadata import TableMetadata
from .explain import ExplainResult


SYSTEM_PROMPT = """You are a Trino SQL optimization assistant.
You must preserve query semantics.
You may only return a single JSON object with keys:
- optimized_sql (string)
- changes (array of short strings)
- assumptions (array of short strings)
- risk (one of: low, medium, high)

Rules:
- Output valid Trino SQL.
- Prefer adding partition predicates when possible (based on provided partition candidates).
- Avoid wrapping partition columns in functions in WHERE (keep predicates sargable).
- Do NOT add new tables.
- Do NOT remove required filters if present.
- Do NOT output markdown or explanations outside JSON.
"""


def _metadata_to_compact_json(metas: List[TableMetadata]) -> str:
    payload = []
    for tm in metas:
        payload.append({
            "table": tm.table.fqtn(),
            "partition_candidates": tm.partition_columns,
            "columns": [{"name": c.name, "type": c.type} for c in tm.columns[:200]],
            "properties_hint": tm.properties,
        })
    return json.dumps(payload, ensure_ascii=False)


def build_optimizer_prompt(
    original_sql: str,
    explain_before: ExplainResult,
    metas: List[TableMetadata],
) -> str:
    meta_json = _metadata_to_compact_json(metas)

    user_prompt = f"""
Optimize this Trino SQL query.

ORIGINAL_SQL:
{original_sql}

EXPLAIN_PLAN_BEFORE:
{explain_before.text[:12000]}

TABLE_METADATA_JSON:
{meta_json[:12000]}

Guidance:
- If query filters on timestamps but tables have date-like partition candidates (e.g., ds/event_date/dt),
  add an additional partition predicate that matches the timestamp range.
- Keep LIMIT when present; if missing and query is obviously exploratory, add a reasonable LIMIT like 100
- Prefer explicit column selection instead of SELECT * when it does not change semantics (be careful with SELECT * used by downstream).
- Keep correctness the highest priority.
- use CTE when there are multiple subquery references to avoid repeating predicates.
- CTEs in Trino are often inlined; they won’t always reduce repeated work. Use CTEs to avoid logic duplication, and validate performance via EXPLAIN ANALYZE.
- keep the smaller table on left side of join
- instead of distinct use approx_distinct and similarly other approx functions wherever applicable.
- show warning when using union
- avoid select * rather add column names
- push down filter and predicate 
- Avoid expressions like date(ts) = DATE '...' or substr(ds,1,10)=... on filter/partition columns; rewrite to range predicates on the raw column.
- If there are many OR conditions on the same column, consider IN (...) or joining to a small values table/CTE.
- For selective dimension-to-fact joins, ensure join keys and filters allow dynamic filtering (and avoid constructs that block it).
- Broadcast (replicated) joins are usually best when one side is small; partitioned joins are better when both sides are large. Prefer writing queries that keep the small side small (filter/projection).
- If the final output is aggregated by a dimension, aggregate the fact table first, then join to dimensions.
- When ordering huge datasets to get a small top set, keep ORDER BY paired with LIMIT; avoid ORDER BY without LIMIT for exploratory use.
- Avoid joining on derived expressions or high-entropy composite keys unless necessary; prefer normalized keys and equality joins.
- If one key value dominates (hot key), consider filtering it separately, salting keys, or restructuring the query to avoid one-task bottlenecks.
- If DISTINCT is used only to remove duplicate entities, dedupe using a key with GROUP BY or window functions (row_number) rather than DISTINCT on wide rows.
- When using window functions, partition by the smallest necessary key set and filter early; avoid large ORDER BY windows over massive partitions.



Return ONLY JSON as specified.
"""
    return user_prompt.strip()


def build_fix_prompt(
    original_sql: str,
    candidate_sql: str,
    error_or_feedback: str,
    explain_before: ExplainResult,
    metas: List[TableMetadata],
) -> str:
    meta_json = _metadata_to_compact_json(metas)

    user_prompt = f"""
You produced an optimized SQL but it failed validation or did not improve.

ORIGINAL_SQL:
{original_sql}

CANDIDATE_SQL:
{candidate_sql}

VALIDATION_ERROR_OR_FEEDBACK:
{error_or_feedback}

EXPLAIN_PLAN_BEFORE:
{explain_before.text[:12000]}

TABLE_METADATA_JSON:
{meta_json[:12000]}

Task:
- Fix the SQL so it is valid Trino SQL.
- Preserve semantics.
- Prefer partition pruning improvements when safe.

Return ONLY JSON as specified.
"""
    return user_prompt.strip()
