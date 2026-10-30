"""Publish simulated events to Kafka: python -m stream.producer [--bootstrap localhost:9092]"""
from __future__ import annotations

import argparse
import json
import time

from confluent_kafka import Producer

from .events import simulate

TOPIC = "parcel_events"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--rate", type=float, default=500.0, help="messages per second")
    args = parser.parse_args()
    producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True})
    for i, message in enumerate(simulate()):
        key = json.loads(message).get("parcel_id", "")  # keyed by parcel so its events stay ordered
        producer.produce(TOPIC, key=key, value=message)
        if i % 1000 == 0:
            producer.poll(0)
        time.sleep(1 / args.rate)
    producer.flush()


if __name__ == "__main__":
    main()
