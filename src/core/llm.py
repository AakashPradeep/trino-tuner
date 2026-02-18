from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from openai import OpenAI

from .settings import Settings
from .prompt import SYSTEM_PROMPT


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
    def __init__(self, s: Settings):
        self._client = OpenAI(api_key=s.openai_api_key)
        self._model = s.openai_model
        self._temp = s.openai_temperature

    def optimize(self, user_prompt: str) -> LLMResult:
        try:
            resp = self._client.chat.completions.create(
                model=self._model,
                temperature=self._temp,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
            )
            text = resp.choices[0].message.content or ""
            parsed = _parse_json_strict(text)
            return LLMResult(
                ok=True,
                optimized_sql=parsed.get("optimized_sql"),
                changes=parsed.get("changes", []),
                assumptions=parsed.get("assumptions", []),
                risk=parsed.get("risk"),
                raw_text=text,
            )
        except Exception as e:
            return LLMResult(ok=False, error=str(e))

def _parse_json_strict(text: str) -> Dict[str, Any]:
    # If model ever returns extra whitespace/newlines, json.loads still works.
    # If it returns code fences, strip them defensively.
    t = text.strip()
    if t.startswith("```"):
        t = t.strip("`")
        # try to remove leading language token
        t = t.split("\n", 1)[-1].strip()
    return json.loads(t)
