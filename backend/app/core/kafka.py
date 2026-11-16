import json
from enum import StrEnum
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from app.core.config import kafka_settings


class KafkaTopic(StrEnum):
    DOCUMENT_UPLOADED = "document.uploaded"


_producer: AIOKafkaProducer | None = None


async def init_kafka_producer() -> None:
    global _producer
    _producer = AIOKafkaProducer(
        bootstrap_servers=kafka_settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        enable_idempotence=True,
    )
    await _producer.start()


async def close_kafka_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


def get_kafka_producer() -> AIOKafkaProducer:
    if _producer is None:
        raise RuntimeError("Kafka producer is not initialized")
    return _producer


async def publish_event(
    topic: KafkaTopic,
    payload: dict[str, Any],
    key: str | None = None,
) -> None:
    producer = get_kafka_producer()
    await producer.send_and_wait(topic=topic.value, value=payload, key=key)


def create_kafka_consumer(
    *topics: KafkaTopic,
    group_id: str = kafka_settings.KAFKA_DOCUMENT_GROUP_ID,
) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        *(t.value for t in topics),
        bootstrap_servers=kafka_settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )