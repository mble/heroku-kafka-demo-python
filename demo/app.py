"""Main application module."""

import asyncio
import signal
from datetime import datetime

from config import Config
from consumer import consume_messages
from kafka_message import MessageBuffer
from producer import produce_message
from quart import Quart, jsonify, render_template, request

cfg = Config()
cfg.validate()

app = Quart(__name__)
buffer = MessageBuffer(10)
shutdown_event = asyncio.Event()


@app.route("/")
async def index():
    return await render_template(
        "index.html", baseurl=request.base_url.strip("/"), topic=cfg.kafka.topic,
    )


@app.route("/messages", methods=["GET"])
async def messages():
    messages = []
    for msg in buffer:
        parsed = msg.to_dict()
        parsed["metadata"]["receivedAt"] = datetime.fromtimestamp(
            parsed["metadata"]["receivedAt"] / 1000,
        ).isoformat()
        messages.append(parsed)
    return jsonify(messages)


@app.route(f"/messages/{cfg.kafka.topic}", methods=["POST"])
async def post_message():
    msg = await request.get_data(as_text=True)
    await produce_message(msg)
    return jsonify({"status": "ok"})


async def shutdown(signal, loop):
    print(f"Received exit signal {signal.name}...")
    shutdown_event.set()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    [task.cancel() for task in tasks]

    print(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


def main():
    import hypercorn.asyncio
    from hypercorn.config import Config as HypercornConfig

    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

    hypercorn_cfg = HypercornConfig()
    hypercorn_cfg.bind = [f"localhost:{cfg.web.port}"]

    try:
        loop.create_task(
            hypercorn.asyncio.serve(
                app, hypercorn_cfg, shutdown_trigger=shutdown_event.wait,
            ),
        )
        loop.create_task(consume_messages(cfg, buffer))
        loop.run_forever()
    finally:
        loop.close()


if __name__ == "__main__":
    main()
