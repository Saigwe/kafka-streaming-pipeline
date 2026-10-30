"""Consume parcel events from Kafka into the Processor: python -m stream.consumer [--bootstrap localhost:9092]

At-least-once: offsets are committed only after a message has been processed, and the Processor deduplicates
redeliveries by event_id. Invalid and late messages are published to the dead-letter topic.
"""
from __future__ import annotations

import argparse
import json

from confluent_kafka import Consumer, Producer

from .pipeline import Processor
from .sink import DuckDBSink

TOPIC, DLQ_TOPIC = "parcel_events", "parcel_events_dlq"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--db", default="output/kpis.duckdb")
    args = parser.parse_args()
    consumer = Consumer({"bootstrap.servers": args.bootstrap, "group.id": "parcel-kpis",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    dlq = Producer({"bootstrap.servers": args.bootstrap})
    processor = Processor(sink=DuckDBSink(args.db))
    consumer.subscribe([TOPIC])
    sent_dlq = sent_alerts = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            processor.handle(msg.value())
            for dead in processor.dead_letters[sent_dlq:]:
                dlq.produce(DLQ_TOPIC, value=json.dumps({"reason": dead["reason"], "payload": str(dead["payload"])}))
            sent_dlq = len(processor.dead_letters)
            for alert in processor.alerts[sent_alerts:]:
                print("ALERT", alert.message())
            sent_alerts = len(processor.alerts)
            consumer.commit(message=msg, asynchronous=True)
    except KeyboardInterrupt:
        processor.finish()
    finally:
        dlq.flush()
        consumer.close()


if __name__ == "__main__":
    main()
