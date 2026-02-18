from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from .parser import TableRef
from .trino_client import TrinoClient


@dataclass
class ColumnInfo:
    name: str
    type: str


@dataclass
class TableMetadata:
    table: TableRef
    columns: List[ColumnInfo]
    # optional hints
    partition_columns: List[str]
    properties: Dict[str, str]


def _split_fqtn_for_trino(table: TableRef, default_catalog: str, default_schema: str) -> TableRef:
    # fill missing catalog/schema from defaults (useful for unqualified references)
    return TableRef(
        catalog=table.catalog or default_catalog,
        schema=table.schema or default_schema,
        table=table.table,
    )


def fetch_table_columns(client: TrinoClient, table: TableRef, default_catalog: str, default_schema: str) -> List[ColumnInfo]:
    t = _split_fqtn_for_trino(table, default_catalog, default_schema)
    # Trino DESCRIBE returns: Column | Type | Extra | Comment (varies)
    rows = client.query(f"DESCRIBE {t.fqtn()}")
    cols: List[ColumnInfo] = []
    for r in rows:
        if not r or not r[0]:
            continue
        col_name = str(r[0])
        col_type = str(r[1]) if len(r) > 1 else "unknown"
        cols.append(ColumnInfo(name=col_name, type=col_type))
    return cols


def fetch_table_properties_best_effort(client: TrinoClient, table: TableRef, default_catalog: str, default_schema: str) -> Dict[str, str]:
    """
    Best-effort properties retrieval via SHOW CREATE TABLE.
    Parsing connector-specific partition info reliably is complex; we treat it as hints.
    """
    t = _split_fqtn_for_trino(table, default_catalog, default_schema)
    rows = client.query(f"SHOW CREATE TABLE {t.fqtn()}")
    # typically one row with one big string
    ddl = "\n".join(str(r[0]) for r in rows if r and r[0])
    props: Dict[str, str] = {}
    if not ddl:
        return props

    # naive extraction of WITH (...) key/value pairs
    # (You can improve this by parsing the WITH block properly.)
    if "WITH (" in ddl:
        props["has_with_properties"] = "true"
    props["create_table_snippet"] = ddl[:2000]  # cap for prompt
    return props


def infer_partition_columns_from_properties(props: Dict[str, str], columns: List[ColumnInfo]) -> List[str]:
    """
    Placeholder heuristic:
    - If table has common partition-like columns, surface them as candidates.
    In real deployments:
      - Iceberg: query <table>$partitions
      - Hive: show create table partitions + properties
    """
    candidates = []
    colnames = {c.name.lower() for c in columns}
    for likely in ["ds", "date", "event_date", "dt", "day", "hour", "event_hour", "partition_date"]:
        if likely in colnames:
            candidates.append(likely)
    return candidates


def fetch_metadata_for_tables(
    client: TrinoClient,
    tables: List[TableRef],
    default_catalog: str,
    default_schema: str,
) -> List[TableMetadata]:
    out: List[TableMetadata] = []
    for t in tables:
        cols = fetch_table_columns(client, t, default_catalog, default_schema)
        props = fetch_table_properties_best_effort(client, t, default_catalog, default_schema)
        part_cols = infer_partition_columns_from_properties(props, cols)
        out.append(TableMetadata(table=_split_fqtn_for_trino(t, default_catalog, default_schema), columns=cols, partition_columns=part_cols, properties=props))
    return out
