"""Kafka message model."""

from dataclasses import asdict, dataclass
from typing import Any, Iterator, Self


@dataclass
class KafkaMessageMetadata:
    """Kafka message metadata."""

    receivedAt: int


@dataclass
class KafkaMessage:
    """Kafka message."""

    value: str
    partition: int
    offset: int
    metadata: KafkaMessageMetadata

    def to_dict(self: Self) -> dict[str, Any]:
        """Convert to dict."""
        return asdict(self)


class MessageBuffer:
    """Message buffer."""

    def __init__(self: Self, max_size: int) -> None:
        """Initialize buffer."""
        self.max_size = max_size
        self.buffer: list[KafkaMessage] = []

    def __iter__(self: Self) -> Iterator[KafkaMessage]:
        """Iterate over buffer."""
        return iter(self.buffer)

    def append(self: Self, msg: KafkaMessage) -> None:
        """Append message to buffer, keeping buffer at max size."""
        self.buffer.append(msg)
        if len(self.buffer) > self.max_size:
            self.buffer.pop(0)
