import os
import json
import logging
import requests
from typing import List, Dict, Any, Optional


logger = logging.getLogger(__name__)


class TaskClassifier:
    """
    AI classifier using Azure OpenAI.
    Returns category, subcategory, impact, urgency (all from allowed lists).
    """

    def __init__(self):
        self.endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        self.api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self.deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")
        self.api_version = os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview")

        if not self.endpoint or not self.api_key or not self.deployment:
            raise RuntimeError("Azure OpenAI env vars missing: AZURE_OPENAI_ENDPOINT / AZURE_OPENAI_API_KEY / AZURE_OPENAI_DEPLOYMENT")

    def _call_azure_openai(self, messages: List[Dict[str, str]]) -> str:
        url = f"{self.endpoint}/openai/deployments/{self.deployment}/chat/completions?api-version={self.api_version}"
        headers = {
            "Content-Type": "application/json",
            "api-key": self.api_key,
        }
        payload = {
            "messages": messages,
            "temperature": 0.1,
            "max_tokens": 400,
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]

    @staticmethod
    def _extract_json(text: str) -> Dict[str, Any]:
        # Try direct JSON
        try:
            return json.loads(text)
        except Exception:
            pass

        # Try to extract the first JSON block
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start:end + 1])

        raise ValueError("Model did not return valid JSON")

    def classify(self,
                 description: str,
                 category_choices: List[str],
                 subcategory_choices: List[str],
                 impact_choices: List[str],
                 urgency_choices: List[str]) -> Dict[str, Any]:
        if not description:
            return {
                "status": "FAILED",
                "summary": "Empty description",
                "details": {}
            }

        system = (
            "You are a ServiceNow incident classifier. "
            "Choose EXACT values from the provided lists. "
            "Return ONLY JSON with keys: category, subcategory, impact, urgency, confidence. "
            "confidence must be a number between 0 and 1."
        )

        user = {
            "description": description,
            "category_choices": category_choices,
            "subcategory_choices": subcategory_choices,
            "impact_choices": impact_choices,
            "urgency_choices": urgency_choices
        }

        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user)}
        ]

        raw = self._call_azure_openai(messages)
        result = self._extract_json(raw)

        # Normalize and validate
        category = result.get("category")
        subcategory = result.get("subcategory")
        impact = result.get("impact")
        urgency = result.get("urgency")
        confidence_raw = result.get("confidence", 0.0)
        try:
            confidence = float(confidence_raw)
        except Exception:
            if isinstance(confidence_raw, str):
                norm = confidence_raw.strip().lower()
                if norm == "high":
                    confidence = 0.9
                elif norm == "medium":
                    confidence = 0.6
                elif norm == "low":
                    confidence = 0.3
                else:
                    confidence = 0.0
            else:
                confidence = 0.0

        if category not in category_choices:
            raise ValueError(f"Model returned invalid category: {category}")
        if subcategory and subcategory not in subcategory_choices:
            raise ValueError(f"Model returned invalid subcategory: {subcategory}")
        if impact not in impact_choices:
            raise ValueError(f"Model returned invalid impact: {impact}")
        if urgency not in urgency_choices:
            raise ValueError(f"Model returned invalid urgency: {urgency}")

        return {
            "status": "SUCCESS",
            "details": {
                "category": category,
                "subcategory": subcategory,
                "impact": impact,
                "urgency": urgency,
                "confidence": confidence
            }
        }
