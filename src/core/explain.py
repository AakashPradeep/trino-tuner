from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Optional

from .trino_client import TrinoClient


@dataclass
class ExplainResult:
    ok: bool
    text: str
    error: Optional[str] = None
    # optional score signals
    estimated_rows: Optional[float] = None
    estimated_cpu: Optional[str] = None


def run_explain(client: TrinoClient, sql: str) -> ExplainResult:
    """
    Runs EXPLAIN and returns the plan text.
    Trino EXPLAIN output is a table; usually first column is the plan string.
    """
    try:
        rows = client.query(f"EXPLAIN {sql}")
        plan = "\n".join(str(r[0]) for r in rows if r and r[0] is not None)
        res = ExplainResult(ok=True, text=plan)
        _populate_signals(res)
        return res
    except Exception as e:
        return ExplainResult(ok=False, text="", error=str(e))


def _populate_signals(res: ExplainResult) -> None:
    """
    Best-effort extraction. Trino plan formats vary.
    We keep this conservative: if we can't parse signals, leave None.
    """
    txt = res.text or ""
    # Rows estimates sometimes appear like "rows: 1.23E6" or "Estimates: {rows: ...}"
    m = re.search(r"rows:\s*([0-9.eE+]+)", txt)
    if m:
        try:
            res.estimated_rows = float(m.group(1))
        except Exception:
            pass
