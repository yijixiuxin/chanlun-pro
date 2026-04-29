from dataclasses import dataclass
from typing import Any

import openai

from chanlun import config


@dataclass(frozen=True)
class AIConfig:
    provider: str
    api_url: str
    api_key: str
    model: str


def resolve_ai_config(config_module=config) -> AIConfig:
    provider = getattr(config_module, "AI_PROVIDER", "").strip().lower()
    api_url = getattr(config_module, "AI_API_URL", "").strip()
    api_key = getattr(config_module, "AI_API_KEY", "").strip()
    model = getattr(config_module, "AI_MODEL", "").strip()
    return AIConfig(provider=provider, api_url=api_url, api_key=api_key, model=model)


def request_ai_model(
    prompt: str,
    *,
    config_module=config,
    client_factory=openai.OpenAI,
    response_format: dict[str, Any] | None = None,
    temperature: float | None = None,
    max_tokens: int | None = None,
) -> dict:
    ai_config = resolve_ai_config(config_module)
    if ai_config.api_key == "" or ai_config.model == "":
        return {
            "ok": False,
            "msg": "未正确配置大模型的 API key 和模型名称",
            "model": ai_config.model,
        }

    try:
        client = client_factory(api_key=ai_config.api_key, base_url=ai_config.api_url)
        kwargs = {
            "model": ai_config.model,
            "messages": [{"role": "user", "content": prompt}],
        }
        if response_format is not None:
            kwargs["response_format"] = response_format
        if temperature is not None:
            kwargs["temperature"] = temperature
        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens

        response = client.chat.completions.create(**kwargs)
        message = response.choices[0].message
        content = message.content or ""
        refusal = getattr(message, "refusal", None)
        if content == "" and refusal is not None:
            return {
                "ok": False,
                "msg": f"**[OpenAI API 错误]**: {refusal}",
                "model": ai_config.model,
            }
        return {"ok": True, "msg": content, "model": ai_config.model}
    except openai.OpenAIError as oe:
        return {
            "ok": False,
            "msg": f"**[OpenAI API 错误]**: {str(oe)}",
            "model": ai_config.model,
        }
    except Exception as e:
        return {
            "ok": False,
            "msg": f"**[系统异常]**: {str(e)}",
            "model": ai_config.model,
        }
