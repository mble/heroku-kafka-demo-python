"""Main application module."""

import asyncio
import logging
import signal
from datetime import datetime, timezone

from quart import Quart, jsonify, render_template, request

from .config import Config
from .consumer import consume_messages
from .kafka_message import MessageBuffer
from .producer import produce_message

cfg = Config()
cfg.validate()

app = Quart(__name__)
buffer = MessageBuffer(10)
shutdown_event = asyncio.Event()

logger = app.logger
app.logger.setLevel(logging.INFO)


@app.route("/")
async def index() -> str:
    """Index handler, parses the index.html template."""
    return await render_template(
        "index.html",
        baseurl=request.base_url.strip("/"),
        topic=cfg.kafka.topic,
    )


@app.route("/messages", methods=["GET"])
async def messages() -> str:
    """Messages handler, returns the contents of the message buffer."""
    messages = []
    for msg in buffer:
        parsed = msg.to_dict()
        parsed["metadata"]["receivedAt"] = datetime.fromtimestamp(
            parsed["metadata"]["receivedAt"] / 1000,
            tz=timezone.utc,
        ).isoformat()
        messages.append(parsed)
    return jsonify(messages)


@app.route(f"/messages/{cfg.kafka.topic}", methods=["POST"])
async def post_message() -> str:
    """Post message handler, sends the message to the Kafka topic."""
    msg = await request.get_data(as_text=True)
    await produce_message(cfg, msg)
    return jsonify({"status": "ok"})


async def shutdown(
    signal: signal,
    loop: asyncio.AbstractEventLoop,
    logger: logging.Logger,
) -> None:
    """Shutdown handler."""
    logger.log(logging.INFO, "Received exit signal %s...", signal.name)
    shutdown_event.set()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    [task.cancel() for task in tasks]

    logger.log(logging.INFO, "Cancelling %d outstanding tasks", len(tasks))
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


def main() -> None:
    """Entrypoint function."""
    import hypercorn.asyncio
    from hypercorn.config import Config as HypercornConfig

    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s,
            lambda s=s: asyncio.create_task(shutdown(s, loop, logger)),
        )

    # hypercorn_cfg = HypercornConfig()
    # hypercorn_cfg.bind = [f"localhost:{cfg.web.port}"]

    try:
        loop.create_task(consume_messages(cfg, buffer))
        loop.create_task(app.run_task(host="0.0.0.0", port=cfg.web.port, shutdown_trigger=shutdown_event.wait))
        loop.run_forever()
    finally:
        loop.close()


if __name__ == "__main__":
    main()
