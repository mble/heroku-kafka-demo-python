"""Configuration for the demo application."""

import os
import ssl
from base64 import standard_b64encode
from dataclasses import dataclass
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


@dataclass
class KafkaConfig:
    """Kafka configuration."""
    
    bootstrap_servers: str
    topic: str
    group_id: str
    prefix: str
    client_cert: str
    client_cert_key: str
    trusted_cert: str

    def __post_init__(self):
        if self.prefix:
            self.topic = f"{self.prefix}{self.topic}"
            self.group_id = f"{self.prefix}{self.group_id}"


@dataclass
class WebConfig:
    """Web configuration."""

    port: str


@dataclass
class Config:
    """Application configuration."""

    kafka: KafkaConfig
    web: WebConfig

    def __init__(self):
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

    def validate(self) -> None:
        if not self.kafka.bootstrap_servers:
            raise ValueError("KAFKA_URL is required")
        if not self.kafka.client_cert:
            raise ValueError("KAFKA_CLIENT_CERT is required")
        if not self.kafka.client_cert_key:
            raise ValueError("KAFKA_CLIENT_CERT_KEY is required")
        if not self.kafka.trusted_cert:
            raise ValueError("KAFKA_TRUSTED_CERT is required")

    def broker_list(self) -> list[str]:
        urls = self.kafka.bootstrap_servers.split(",")
        addrs = []

        for url in urls:
            u = urlparse(url)
            addrs.append(f"{u.hostname}:{u.port}")

        return addrs

    def create_ssl_context(self) -> ssl.SSLContext:
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
                ssl.Purpose.SERVER_AUTH, cafile=trust_file.name,
            )
            ssl_context.load_cert_chain(
                cert_file.name, keyfile=key_file.name, password=passwd,
            )

            ssl_context.check_hostname = False

        return ssl_context
