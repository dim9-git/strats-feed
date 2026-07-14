"""Minimal Kafka helpers (kafka-python)."""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any, TypeVar

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)
T = TypeVar("T")


def _new_id() -> str:
    return str(uuid.uuid4())


@dataclass(frozen=True)
class BarsFetchedEvent:
    """BTC 1h feed event. Hourly strategies use ``bars``; daily use ``bars_daily``.

    Published every new closed 1h bar. ``new_daily_bar`` is true only when the
    UTC daily bar just advanced (daily strategies should gate on this).
    """

    symbol: str
    timeframe: str
    market: str
    last_closed_bar: str
    fetched_at: str
    lookback_days: int
    bars: list[dict[str, Any]]
    bars_daily: list[dict[str, Any]] = field(default_factory=list)
    last_closed_daily_bar: str = ""
    new_daily_bar: bool = False
    with_metrics: bool = True
    with_funding: bool = True
    bar_count: int | None = None
    daily_bar_count: int | None = None
    event_id: str = field(default_factory=_new_id)
    event_type: str = "bars.fetched"

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, raw: bytes | str) -> BarsFetchedEvent:
        data = json.loads(raw)
        return cls(
            symbol=data["symbol"],
            timeframe=data["timeframe"],
            market=data["market"],
            last_closed_bar=data["last_closed_bar"],
            fetched_at=data["fetched_at"],
            lookback_days=int(data["lookback_days"]),
            bars=list(data.get("bars") or []),
            bars_daily=list(data.get("bars_daily") or []),
            last_closed_daily_bar=data.get("last_closed_daily_bar", ""),
            new_daily_bar=bool(data.get("new_daily_bar", False)),
            with_metrics=bool(data.get("with_metrics", True)),
            with_funding=bool(data.get("with_funding", True)),
            bar_count=data.get("bar_count"),
            daily_bar_count=data.get("daily_bar_count"),
            event_id=data.get("event_id", _new_id()),
            event_type=data.get("event_type", "bars.fetched"),
        )


class KafkaBus:
    def __init__(self, bootstrap_servers: str) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS is required")
        self.bootstrap_servers = bootstrap_servers

    def producer(self) -> KafkaProducer:
        try:
            return KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                acks="all",
                retries=5,
                linger_ms=20,
                max_request_size=5_000_000,
                value_serializer=lambda v: v if isinstance(v, (bytes, bytearray)) else bytes(v),
                key_serializer=lambda k: None
                if k is None
                else (k if isinstance(k, bytes) else str(k).encode()),
            )
        except KafkaError as exc:
            raise RuntimeError(f"No Kafka brokers at {self.bootstrap_servers!r}") from exc

    def consumer(
        self,
        topics: list[str],
        *,
        group_id: str,
        auto_offset_reset: str = "latest",
    ) -> KafkaConsumer:
        try:
            return KafkaConsumer(
                *topics,
                bootstrap_servers=self.bootstrap_servers.split(","),
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=True,
                value_deserializer=lambda v: v,
                key_deserializer=lambda k: None if k is None else k.decode(),
                consumer_timeout_ms=1000,
                max_partition_fetch_bytes=5_000_000,
            )
        except KafkaError as exc:
            raise RuntimeError(f"No Kafka brokers at {self.bootstrap_servers!r}") from exc


def publish(producer: KafkaProducer, topic: str, value: bytes, *, key: str | None = None) -> None:
    producer.send(topic, value=value, key=key).get(timeout=30)


def consume_forever(
    consumer: KafkaConsumer,
    parse: Callable[[bytes], T],
    handle: Callable[[T], None],
    *,
    idle_log_every: int = 60,
) -> None:
    idle_ticks = 0
    try:
        while True:
            polled = consumer.poll(timeout_ms=1000)
            if not polled:
                idle_ticks += 1
                if idle_log_every and idle_ticks % idle_log_every == 0:
                    logger.info("Waiting for messages…")
                continue
            idle_ticks = 0
            for _tp, records in polled.items():
                for record in records:
                    try:
                        handle(parse(record.value))
                    except Exception:
                        logger.exception(
                            "Failed handling message topic=%s offset=%s",
                            record.topic,
                            record.offset,
                        )
    finally:
        consumer.close()
