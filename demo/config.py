"""Configuration for the demo application."""
from __future__ import annotations

import os
import ssl
from base64 import standard_b64encode
from dataclasses import dataclass
from tempfile import NamedTemporaryFile
from typing import Self
from urllib.parse import urlparse

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


@dataclass
class KafkaConfig:
    """Kafka configuration."""

    bootstrap_servers: str | None
    topic: str
    group_id: str
    prefix: str | None
    client_cert: str | None
    client_cert_key: str | None
    trusted_cert: str | None

    def __post_init__(self: Self) -> None:
        """Post-initialization hook."""
        if self.prefix:
            self.topic = f"{self.prefix}{self.topic}"
            self.group_id = f"{self.prefix}{self.group_id}"


@dataclass
class WebConfig:
    """Web configuration."""

    port: str


class ConfigRequiredError(Exception):
    """Exception raised when a required configuration value is missing."""

    def __init__(self: Self, envvar: str) -> None:
        """Initialize exception."""
        super().__init__(f"{envvar} is required")


@dataclass
class Config:
    """Application configuration."""

    kafka: KafkaConfig
    web: WebConfig

    def __init__(self: Self) -> None:
        """Initialize configuration."""
        self.kafka = KafkaConfig(
            bootstrap_servers=os.getenv("KAFKA_URL"),
            topic=os.getenv("KAFKA_TOPIC", "messages"),
            group_id=os.getenv("KAFKA_CONSUMER_GROUP", "heroku-kafka-demo-python"),
            prefix=os.getenv("KAFKA_PREFIX"),
            client_cert=os.getenv("KAFKA_CLIENT_CERT"),
            client_cert_key=os.getenv("KAFKA_CLIENT_CERT_KEY"),
            trusted_cert=os.getenv("KAFKA_TRUSTED_CERT"),
        )
        self.web = WebConfig(port=os.getenv("PORT", "8000"))

    def validate(self: Self) -> None:
        """Validate configuration."""
        if not self.kafka.bootstrap_servers:
            key = "KAFKA_URL"
            raise ConfigRequiredError(key)
        if not self.kafka.client_cert:
            key = "KAFKA_CLIENT_CERT"
            raise ConfigRequiredError(key)
        if not self.kafka.client_cert_key:
            key = "KAFKA_CLIENT_CERT_KEY"
            raise ConfigRequiredError(key)
        if not self.kafka.trusted_cert:
            key = "KAFKA_TRUSTED_CERT"
            raise ConfigRequiredError(key)

    def broker_list(self: Self) -> list[str]:
        """Return Kafka broker list, parsed from KAFKA_URL."""
        if not self.kafka.bootstrap_servers:
            raise ConfigRequiredError("KAFKA_URL")

        urls = self.kafka.bootstrap_servers.split(",")
        addrs: list[str] = []

        for url in urls:
            u = urlparse(url)
            addrs.append(f"{u.hostname}:{u.port}")

        return addrs

    def create_ssl_context(self: Self) -> ssl.SSLContext:
        """Create SSL context."""
        if not self.kafka.client_cert:
            raise ConfigRequiredError("KAFKA_CLIENT_CERT")

        if not self.kafka.client_cert_key:
            raise ConfigRequiredError("KAFKA_CLIENT_CERT_KEY")

        if not self.kafka.trusted_cert:
            raise ConfigRequiredError("KAFKA_TRUSTED_CERT")

        with NamedTemporaryFile(suffix=".crt") as cert_file, NamedTemporaryFile(
            suffix=".key",
        ) as key_file, NamedTemporaryFile(suffix=".crt") as trust_file:
            cert_file.write(self.kafka.client_cert.encode("utf-8"))
            cert_file.flush()

            passwd = standard_b64encode(os.urandom(32)).decode("utf-8")
            private_key = serialization.load_pem_private_key(
                self.kafka.client_cert_key.encode("utf-8"),
                password=None,
                backend=default_backend(),
            )
            pem = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.BestAvailableEncryption(
                    passwd.encode("utf-8"),
                ),
            )
            key_file.write(pem)
            key_file.flush()

            trust_file.write(self.kafka.trusted_cert.encode("utf-8"))
            trust_file.flush()

            ssl_context = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH,
                cafile=trust_file.name,
            )
            ssl_context.load_cert_chain(
                cert_file.name,
                keyfile=key_file.name,
                password=passwd,
            )

            ssl_context.check_hostname = False

        return ssl_context
