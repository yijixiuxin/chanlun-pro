from types import SimpleNamespace

from chanlun.tools.ai_client import request_ai_model, resolve_ai_config


class FakeMessage:
    content = "AI response"
    refusal = None


class FakeCompletions:
    def __init__(self):
        self.kwargs = None

    def create(self, **kwargs):
        self.kwargs = kwargs
        return SimpleNamespace(choices=[SimpleNamespace(message=FakeMessage())])


class FakeClient:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url
        self.chat = SimpleNamespace(completions=FakeCompletions())


def test_resolve_ai_config_prefers_unified_config_and_provider_url():
    cfg = SimpleNamespace(
        AI_PROVIDER="deepseek",
        AI_API_URL="",
        AI_API_KEY="deepseek-key",
        AI_MODEL="deepseek-chat",
    )

    ai_config = resolve_ai_config(cfg)

    assert ai_config.provider == "deepseek"
    assert ai_config.api_url == "https://api.deepseek.com"
    assert ai_config.api_key == "deepseek-key"
    assert ai_config.model == "deepseek-chat"


def test_resolve_ai_config_uses_siliconflow_default_url():
    cfg = SimpleNamespace(
        AI_PROVIDER="siliconflow",
        AI_API_URL="",
        AI_API_KEY="sf-key",
        AI_MODEL="Pro/deepseek-ai/DeepSeek-V3",
    )

    ai_config = resolve_ai_config(cfg)

    assert ai_config.api_url == "https://api.siliconflow.cn/v1/"


def test_request_ai_model_uses_openai_compatible_client():
    cfg = SimpleNamespace(
        AI_PROVIDER="openai",
        AI_API_URL="https://example.test/v1",
        AI_API_KEY="key",
        AI_MODEL="model-a",
    )
    clients = []

    def client_factory(**kwargs):
        client = FakeClient(**kwargs)
        clients.append(client)
        return client

    result = request_ai_model(
        "hello",
        config_module=cfg,
        client_factory=client_factory,
        response_format={"type": "json_object"},
        temperature=0.2,
        max_tokens=2048,
    )

    assert result == {"ok": True, "msg": "AI response", "model": "model-a"}
    assert clients[0].api_key == "key"
    assert clients[0].base_url == "https://example.test/v1"
    assert clients[0].chat.completions.kwargs == {
        "model": "model-a",
        "messages": [{"role": "user", "content": "hello"}],
        "response_format": {"type": "json_object"},
        "temperature": 0.2,
        "max_tokens": 2048,
    }


def test_request_ai_model_reports_missing_config():
    cfg = SimpleNamespace(
        AI_PROVIDER="siliconflow",
        AI_API_URL="",
        AI_API_KEY="",
        AI_MODEL="",
    )

    result = request_ai_model("hello", config_module=cfg)

    assert result["ok"] is False
    assert "未正确配置" in result["msg"]
    assert result["model"] == ""
