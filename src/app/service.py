from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

from core.settings import Settings
from core.trino_client import TrinoClient
from core.llm import LLMClient
from core.optimizer import optimize_sql


app = FastAPI(title="Trino SQL Optimizer")


class OptimizeRequest(BaseModel):
    sql: str


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/optimize")
def optimize(req: OptimizeRequest):
    s = Settings()
    trino = TrinoClient.from_settings(s)
    llm = LLMClient(s)

    result = optimize_sql(s=s, client=trino, llm=llm, sql=req.sql)

    # Serialize metadata compactly
    meta_out = []
    for tm in result.metadata:
        meta_out.append({
            "table": tm.table.fqtn(),
            "partition_candidates": tm.partition_columns,
            "columns": [{"name": c.name, "type": c.type} for c in tm.columns],
        })

    return {
        "ok": result.ok,
        "attempts": result.attempts,
        "tables": result.tables,
        "llm": {
            "risk": result.llm_risk,
            "changes": result.llm_changes,
            "assumptions": result.llm_assumptions,
        },
        "original_sql": result.original_sql,
        "optimized_sql": result.optimized_sql,
        "diff": result.diff,
        "explain_before": {"ok": result.explain_before.ok, "error": result.explain_before.error, "text": result.explain_before.text},
        "explain_after": None if not result.explain_after else {"ok": result.explain_after.ok, "error": result.explain_after.error, "text": result.explain_after.text},
        "metadata": meta_out,
        "error": result.error,
    }
