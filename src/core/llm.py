from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .settings import Settings
from .prompt import SYSTEM_PROMPT

from langchain_core.prompts import ChatPromptTemplate


@dataclass
class LLMResult:
    ok: bool
    optimized_sql: Optional[str] = None
    changes: Optional[List[str]] = None
    assumptions: Optional[List[str]] = None
    risk: Optional[str] = None
    raw_text: Optional[str] = None
    error: Optional[str] = None


class LLMClient:
    """
    Backwards-compatible drop-in replacement:
    - Same class name
    - Same constructor signature
    - Same optimize() signature and return type
    - Still returns raw_text like before
    """

    def __init__(self, s: Settings):
        self._settings = s
        self._llm = self._build_llm(s)
        self._prompt = ChatPromptTemplate.from_messages(
            [
                ("system", SYSTEM_PROMPT),
                ("user", "{user_prompt}"),
            ]
        )

    def optimize(self, user_prompt: str) -> LLMResult:
        try:
            # Single model call -> get raw text
            msgs = self._prompt.format_messages(user_prompt=user_prompt)
            resp = self._llm.invoke(msgs)
            text = getattr(resp, "content", str(resp)) or ""

            parsed = _parse_json_strict(text)

            return LLMResult(
                ok=True,
                optimized_sql=parsed.get("optimized_sql"),
                changes=parsed.get("changes", []),
                assumptions=parsed.get("assumptions", []),
                risk=parsed.get("risk"),
                raw_text=text,  # kept exactly like your current impl
            )
        except Exception as e:
            return LLMResult(ok=False, error=str(e))

    @staticmethod
    def _build_llm(s: Settings):
        provider = s.llm_provider.lower()

        if provider == "openai":
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                api_key=s.openai_api_key,
                model=s.openai_model,
                temperature=s.openai_temperature,
            )

        if provider == "azure_openai":
            from langchain_openai import AzureChatOpenAI
            return AzureChatOpenAI(
                api_key=s.azure_openai_api_key,
                azure_endpoint=s.azure_openai_endpoint,
                azure_deployment=s.azure_openai_deployment,
                api_version=s.azure_openai_api_version,
                temperature=s.openai_temperature,
            )

        if provider == "anthropic":
            from langchain_anthropic import ChatAnthropic
            return ChatAnthropic(
                api_key=s.anthropic_api_key,
                model=s.anthropic_model,
                temperature=getattr(s, "anthropic_temperature", s.openai_temperature),
            )

        if provider == "gemini":
            from langchain_google_genai import ChatGoogleGenerativeAI
            return ChatGoogleGenerativeAI(
                google_api_key=s.gemini_api_key,
                model=s.gemini_model,
                temperature=getattr(s, "gemini_temperature", s.openai_temperature),
            )

        raise ValueError(f"Unsupported llm_provider: {provider}")


def _parse_json_strict(text: str) -> Dict[str, Any]:
    t = (text or "").strip()
    if t.startswith("```"):
        # Remove ```lang\n ... \n``` fences
        t = t.strip("`")
        t = t.split("\n", 1)[-1].strip()
    return json.loads(t)