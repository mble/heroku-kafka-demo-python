"""Producer example."""


from aiokafka import AIOKafkaProducer  # type: ignore

from .config import Config


async def produce_message(cfg: Config, msg: str) -> None:
    """Produce message to Kafka."""
    ssl_context = cfg.create_ssl_context()

    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.broker_list(),  # type: ignore
        security_protocol="SSL",
        ssl_context=ssl_context,
        acks="all",
        retry_backoff_ms=200,
    )
    await producer.start()

    try:
        await producer.send_and_wait(  # type: ignore
            cfg.kafka.topic,
            msg.encode("utf-8"),
        )
    finally:
        await producer.stop()
