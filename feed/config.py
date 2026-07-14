"""Feed service configuration — BTC 1h source of truth for all strategies."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
load_dotenv(Path(__file__).resolve().parents[1] / ".env")
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

DEFAULT_KAFKA = "localhost:9092"


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _env_bool(name: str, default: bool) -> bool:
    if name not in os.environ:
        return default
    return os.environ[name].strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class FeedConfig:
    symbol: str = "BTCUSDT"
    timeframe: str = "1h"
    # Enough history to build ~400 daily bars for daily strategies.
    lookback_days: int = 420
    with_metrics: bool = True
    with_funding: bool = True
    market: str = "futures"
    poll_seconds: int = 3600
    run_once: bool = False
    publish_on_unchanged: bool = False
    kafka_bootstrap_servers: str = DEFAULT_KAFKA
    bars_topic: str = "bars.fetched"
    cache_dir: Path = Path("cache")
    publish_bar_count: int = 400  # 1h bars in event
    publish_daily_bar_count: int = 400  # daily bars in event

    @classmethod
    def from_env(cls) -> FeedConfig:
        platform_root = Path(__file__).resolve().parents[1]
        cache_raw = _env("FEED_CACHE_DIR", str(platform_root / "cache"))
        daily_n = int(_env("FEED_PUBLISH_DAILY_BAR_COUNT", "400"))
        lookback_default = str(max(420, daily_n + 20))
        return cls(
            symbol=_env("FEED_SYMBOL", "BTCUSDT"),
            timeframe=_env("FEED_TIMEFRAME", "1h"),
            lookback_days=int(_env("FEED_LOOKBACK_DAYS", lookback_default)),
            with_metrics=_env_bool("FEED_WITH_METRICS", True),
            with_funding=_env_bool("FEED_WITH_FUNDING", True),
            market=_env("FEED_MARKET", "futures"),
            poll_seconds=int(_env("FEED_POLL_SECONDS", "3600")),
            run_once=_env_bool("RUN_ONCE", False),
            publish_on_unchanged=_env_bool("FEED_PUBLISH_ON_UNCHANGED", False),
            kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA),
            bars_topic=_env("BARS_TOPIC", "bars.fetched"),
            cache_dir=Path(cache_raw),
            publish_bar_count=int(_env("FEED_PUBLISH_BAR_COUNT", "500")),
            publish_daily_bar_count=daily_n,
        )

    def to_data_request(self):
        from strats_sdk.platform import DataRequest

        return DataRequest(
            symbol=self.symbol,
            timeframe=self.timeframe,
            lookback_days=self.lookback_days,
            with_metrics=self.with_metrics,
            with_funding=self.with_funding,
            market=self.market,
        )
