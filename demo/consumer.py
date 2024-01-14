"""Consumer example."""

import asyncio

from aiokafka import AIOKafkaConsumer
from config import Config
from kafka_message import KafkaMessage, KafkaMessageMetadata, MessageBuffer

cfg = Config()
cfg.validate()


async def consume_messages(cfg: Config, buffer: MessageBuffer) -> None:
    """Consume messages from Kafka."""
    ssl_context = cfg.create_ssl_context()

    consumer = AIOKafkaConsumer(
        cfg.kafka.topic,
        bootstrap_servers=cfg.broker_list(),
        group_id=cfg.kafka.group_id,
        security_protocol="SSL",
        ssl_context=ssl_context,
        enable_auto_commit=True,
        retry_backoff_ms=200,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            kmsg = KafkaMessage(
                value=msg.value.decode("utf-8"),
                partition=msg.partition,
                offset=msg.offset,
                metadata=KafkaMessageMetadata(receivedAt=msg.timestamp),
            )
            buffer.append(kmsg)
    except asyncio.CancelledError:
        await consumer.stop()
    finally:
        await consumer.stop()
