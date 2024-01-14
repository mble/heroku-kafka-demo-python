import pytest

from demo.config import Config, ConfigRequiredError


class TestConfig:
    """Test configuration."""

    @pytest.fixture
    def _clear_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Clear environment variables."""
        monkeypatch.delenv("KAFKA_URL", raising=False)
        monkeypatch.delenv("KAFKA_CLIENT_CERT", raising=False)
        monkeypatch.delenv("KAFKA_CLIENT_CERT_KEY", raising=False)
        monkeypatch.delenv("KAFKA_TRUSTED_CERT", raising=False)
        monkeypatch.delenv("KAFKA_TOPIC", raising=False)
        monkeypatch.delenv("KAFKA_CONSUMER_GROUP", raising=False)
        monkeypatch.delenv("KAFKA_PREFIX", raising=False)
        monkeypatch.delenv("PORT", raising=False)

    @pytest.fixture
    def _set_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Set environment variables."""
        monkeypatch.setenv("KAFKA_URL", "kafka://localhost:9092")
        monkeypatch.setenv("KAFKA_CLIENT_CERT", "client.crt")
        monkeypatch.setenv("KAFKA_CLIENT_CERT_KEY", "client.key")
        monkeypatch.setenv("KAFKA_TRUSTED_CERT", "ca.crt")
        monkeypatch.setenv("KAFKA_TOPIC", "messages")
        monkeypatch.setenv("KAFKA_CONSUMER_GROUP", "heroku-kafka-demo-python")
        monkeypatch.setenv("PORT", "8000")

    def test_validate_failure(self, _clear_env: None):
        cfg = Config()

        with pytest.raises(ConfigRequiredError) as ex:
            cfg.validate()

        assert str(ex.value) == "KAFKA_URL is required"

    def test_validate_success(self, _clear_env: None, _set_env: None):
        cfg = Config()
        cfg.validate()

        assert cfg.kafka.bootstrap_servers == "kafka://localhost:9092"
        assert cfg.kafka.client_cert == "client.crt"
        assert cfg.kafka.client_cert_key == "client.key"
        assert cfg.kafka.trusted_cert == "ca.crt"
        assert cfg.kafka.topic == "messages"
        assert cfg.kafka.group_id == "heroku-kafka-demo-python"
        assert cfg.kafka.prefix is None
        assert cfg.web.port == "8000"

    def test_validate_success_with_prefix(
        self, _clear_env: None, _set_env: None, monkeypatch: pytest.MonkeyPatch
    ):
        prefix = "test."
        monkeypatch.setenv("KAFKA_PREFIX", prefix)

        cfg = Config()
        cfg.validate()

        assert cfg.kafka.topic == f"{prefix}messages"
        assert cfg.kafka.group_id == f"{prefix}heroku-kafka-demo-python"
        assert cfg.kafka.prefix == prefix

    def test_broker_list(self, _clear_env: None, _set_env: None):
        cfg = Config()
        cfg.validate()

        assert cfg.broker_list() == ["localhost:9092"]
