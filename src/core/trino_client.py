"""
Copyright (C) 2026 Aakash Pradeep
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import trino
from trino.auth import BasicAuthentication

from .settings import Settings


@dataclass
class TrinoConfig:
    host: str
    port: int
    user: str
    catalog: str
    schema: str
    http_scheme: str
    source: str
    session_properties: Dict[str, Any]
    basic_user: Optional[str] = None
    basic_password: Optional[str] = None


class TrinoClient:
    """
    Minimal Trino client wrapper:
    - run SQL (including EXPLAIN)
    - fetch results as rows
    """

    def __init__(self, cfg: TrinoConfig):
        auth = None
        if cfg.basic_user and cfg.basic_password:
            auth = BasicAuthentication(cfg.basic_user, cfg.basic_password)

        self._conn = trino.dbapi.connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            http_scheme=cfg.http_scheme,
            catalog=cfg.catalog,
            schema=cfg.schema,
            source=cfg.source,
            session_properties=cfg.session_properties,
            auth=auth,
        )

    @staticmethod
    def from_settings(s: Settings) -> "TrinoClient":
        cfg = TrinoConfig(
            host=s.trino_host,
            port=s.trino_port,
            user=s.trino_user,
            catalog=s.trino_catalog,
            schema=s.trino_schema,
            http_scheme=s.trino_http_scheme,
            source=s.trino_source,
            session_properties=s.trino_session_props_dict(),
            basic_user=s.trino_basic_user,
            basic_password=s.trino_basic_password,
        )
        return TrinoClient(cfg)

    def query(self, sql: str) -> List[List[Any]]:
        cur = self._conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return rows
