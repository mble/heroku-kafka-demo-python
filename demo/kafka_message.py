"""Kafka message model."""

from dataclasses import asdict, dataclass


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

    def to_dict(self) -> dict:
        return asdict(self)


class MessageBuffer:
    """Message buffer."""

    def __init__(self, max_size: int):
        self.max_size = max_size
        self.buffer = []

    def __iter__(self):
        return iter(self.buffer)

    def append(self, msg: KafkaMessage):
        self.buffer.append(msg)
        if len(self.buffer) > self.max_size:
            self.buffer.pop(0)
