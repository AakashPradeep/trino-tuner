from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp


@dataclass(frozen=True)
class TableRef:
    catalog: Optional[str]
    schema: Optional[str]
    table: str

    def fqtn(self) -> str:
        # Trino commonly uses catalog.schema.table
        parts = [p for p in [self.catalog, self.schema, self.table] if p]
        return ".".join(parts)


def extract_tables_trino(sql: str) -> List[TableRef]:
    """
    Parse SQL using sqlglot 'trino' dialect and extract table references.
    Handles common forms:
      - table
      - schema.table
      - catalog.schema.table

    Note: sqlglot sometimes maps into: catalog=db, db=schema depending on dialect.
    We attempt best-effort extraction.
    """
    tree = sqlglot.parse_one(sql, read="trino")
    seen: Set[Tuple[Optional[str], Optional[str], str]] = set()
    out: List[TableRef] = []

    for t in tree.find_all(exp.Table):
        name = t.name
        db = t.db  # often schema
        catalog = getattr(t, "catalog", None)
        if not name:
            continue
        key = (catalog, db, name)
        if key not in seen:
            seen.add(key)
            out.append(TableRef(catalog=catalog, schema=db, table=name))

    return out
