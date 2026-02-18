from __future__ import annotations

import json
from typing import Any, Dict, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Trino
    trino_host: str = Field(alias="TRINO_HOST")
    trino_port: int = Field(default=443, alias="TRINO_PORT")
    trino_user: str = Field(alias="TRINO_USER")
    trino_catalog: str = Field(default="hive", alias="TRINO_CATALOG")
    trino_schema: str = Field(default="default", alias="TRINO_SCHEMA")
    trino_http_scheme: str = Field(default="https", alias="TRINO_HTTP_SCHEME")

    trino_basic_user: Optional[str] = Field(default=None, alias="TRINO_BASIC_USER")
    trino_basic_password: Optional[str] = Field(default=None, alias="TRINO_BASIC_PASSWORD")

    trino_source: str = Field(default="sql-optimizer", alias="TRINO_SOURCE")
    trino_session_properties: str = Field(default="{}", alias="TRINO_SESSION_PROPERTIES")

    # LLM
    llm_provider: str = Field(default="openai", alias="LLM_PROVIDER")
    ai_api_key: str = Field(alias="AI_API_KEY")
    ai_model: str = Field(default="gpt-4.1-mini", alias="AI_MODEL")
    openai_temperature: float = Field(default=0.0, alias="OPENAI_TEMPERATURE")

    # Optimizer behavior
    max_fix_attempts: int = Field(default=2, alias="MAX_FIX_ATTEMPTS")
    explain_timeout_seconds: int = Field(default=60, alias="EXPLAIN_TIMEOUT_SECONDS")
    read_only_mode: bool = Field(default=True, alias="READ_ONLY_MODE")

    def trino_session_props_dict(self) -> Dict[str, Any]:
        try:
            return json.loads(self.trino_session_properties or "{}")
        except Exception:
            return {}
