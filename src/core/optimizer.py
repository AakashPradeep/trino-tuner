from __future__ import annotations

import difflib
from dataclasses import dataclass
from typing import List, Optional

import sqlglot

from .settings import Settings
from .trino_client import TrinoClient
from .parser import extract_tables_trino
from .metadata import fetch_metadata_for_tables, TableMetadata
from .explain import run_explain, ExplainResult
from .prompt import build_optimizer_prompt, build_fix_prompt
from .llm import LLMClient


@dataclass
class OptimizeResponse:
    ok: bool
    original_sql: str
    optimized_sql: Optional[str]
    explain_before: ExplainResult
    explain_after: Optional[ExplainResult]
    tables: List[str]
    metadata: List[TableMetadata]
    attempts: int
    diff: str
    error: Optional[str] = None
    llm_changes: Optional[List[str]] = None
    llm_assumptions: Optional[List[str]] = None
    llm_risk: Optional[str] = None


def _is_read_only_select(sql: str) -> bool:
    """
    Minimal safeguard: ensure parsed statement is a SELECT/WITH SELECT.
    """
    try:
        tree = sqlglot.parse_one(sql, read="trino")
        sel = tree if tree.__class__.__name__ == "Select" else tree.find(sqlglot.expressions.Select)
        return sel is not None
    except Exception:
        return False


def _diff_text(a: str, b: str) -> str:
    lines = difflib.unified_diff(
        a.splitlines(),
        b.splitlines(),
        fromfile="original.sql",
        tofile="optimized.sql",
        lineterm="",
    )
    return "\n".join(lines)


def _is_improved(before: ExplainResult, after: ExplainResult) -> bool:
    """
    Conservative "improvement" heuristic:
    - if we can parse estimated_rows, require it to not increase
    Otherwise, accept valid plan as 'ok' and return True.
    You can make this smarter later (scan bytes, partitions, cpu, etc.).
    """
    if not after.ok:
        return False
    if before.estimated_rows is not None and after.estimated_rows is not None:
        return after.estimated_rows <= before.estimated_rows * 1.05  # allow small variance
    return True


def optimize_sql(
    s: Settings,
    client: TrinoClient,
    llm: LLMClient,
    sql: str,
) -> OptimizeResponse:
    original_sql = (sql or "").strip()
    if not original_sql:
        return OptimizeResponse(
            ok=False,
            original_sql="",
            optimized_sql=None,
            explain_before=ExplainResult(ok=False, text="", error="Empty SQL"),
            explain_after=None,
            tables=[],
            metadata=[],
            attempts=0,
            diff="",
            error="Empty SQL",
        )

    if s.read_only_mode and not _is_read_only_select(original_sql):
        return OptimizeResponse(
            ok=False,
            original_sql=original_sql,
            optimized_sql=None,
            explain_before=ExplainResult(ok=False, text="", error="Only SELECT queries are allowed in read_only_mode"),
            explain_after=None,
            tables=[],
            metadata=[],
            attempts=0,
            diff="",
            error="Only SELECT queries are allowed in read_only_mode",
        )

    # 1) EXPLAIN original
    explain_before = run_explain(client, original_sql)
    if not explain_before.ok:
        return OptimizeResponse(
            ok=False,
            original_sql=original_sql,
            optimized_sql=None,
            explain_before=explain_before,
            explain_after=None,
            tables=[],
            metadata=[],
            attempts=0,
            diff="",
            error=f"EXPLAIN failed for original SQL: {explain_before.error}",
        )

    # 2) Parse tables
    table_refs = extract_tables_trino(original_sql)
    tables = [t.fqtn() for t in table_refs]

    # 3) Fetch metadata
    metas = fetch_metadata_for_tables(
        client=client,
        tables=table_refs,
        default_catalog=s.trino_catalog,
        default_schema=s.trino_schema,
    )

    # 4) Build prompt
    prompt = build_optimizer_prompt(original_sql, explain_before, metas)

    candidate_sql: Optional[str] = None
    explain_after: Optional[ExplainResult] = None
    llm_changes: Optional[List[str]] = None
    llm_assumptions: Optional[List[str]] = None
    llm_risk: Optional[str] = None
    last_error: Optional[str] = None

    # 5-7) LLM optimize + validate with EXPLAIN + retry fix
    attempts = 0
    for i in range(s.max_fix_attempts + 1):
        attempts = i + 1

        if i == 0:
            res = llm.optimize(prompt)
        else:
            fix_prompt = build_fix_prompt(
                original_sql=original_sql,
                candidate_sql=candidate_sql or "",
                error_or_feedback=last_error or "Unknown failure",
                explain_before=explain_before,
                metas=metas,
            )
            res = llm.optimize(fix_prompt)

        if not res.ok or not res.optimized_sql:
            last_error = res.error or "LLM returned empty output"
            continue

        candidate_sql = res.optimized_sql.strip()
        llm_changes = res.changes or []
        llm_assumptions = res.assumptions or []
        llm_risk = res.risk or "unknown"

        if s.read_only_mode and not _is_read_only_select(candidate_sql):
            last_error = "Candidate SQL is not SELECT-only (read_only_mode)."
            continue

        # 6) EXPLAIN optimized
        explain_after = run_explain(client, candidate_sql)
        if not explain_after.ok:
            last_error = f"EXPLAIN failed: {explain_after.error}"
            continue

        # 6b) check improvement heuristic
        if _is_improved(explain_before, explain_after):
            diff = _diff_text(original_sql, candidate_sql)
            return OptimizeResponse(
                ok=True,
                original_sql=original_sql,
                optimized_sql=candidate_sql,
                explain_before=explain_before,
                explain_after=explain_after,
                tables=tables,
                metadata=metas,
                attempts=attempts,
                diff=diff,
                llm_changes=llm_changes,
                llm_assumptions=llm_assumptions,
                llm_risk=llm_risk,
            )

        last_error = "Candidate SQL did not appear improved based on EXPLAIN signals."

    # 8) Return best effort failure
    diff = _diff_text(original_sql, candidate_sql) if candidate_sql else ""
    return OptimizeResponse(
        ok=False,
        original_sql=original_sql,
        optimized_sql=candidate_sql,
        explain_before=explain_before,
        explain_after=explain_after,
        tables=tables,
        metadata=metas,
        attempts=attempts,
        diff=diff,
        error=last_error or "Failed to produce a valid optimized query",
        llm_changes=llm_changes,
        llm_assumptions=llm_assumptions,
        llm_risk=llm_risk,
    )
