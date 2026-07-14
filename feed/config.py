"""Feed service configuration — BTC 1h source of truth for all strategies."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

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


def _hostport_from_url(raw: str) -> str:
    """Accept ``host:port`` or ``scheme://user:pass@host:port``."""
    raw = raw.strip()
    if not raw:
        return ""
    if "://" not in raw:
        return raw
    scheme, rest = raw.split("://", 1)
    # urlparse rejects schemes with underscores (e.g. SASL_PLAINTEXT)
    parsed = urlparse(f"{scheme.replace('_', '-').lower()}://{rest}")
    if not parsed.hostname:
        return raw
    if parsed.port:
        return f"{parsed.hostname}:{parsed.port}"
    return parsed.hostname


def _creds_from_url(raw: str) -> tuple[str, str]:
    if "://" not in raw:
        return "", ""
    scheme, rest = raw.split("://", 1)
    parsed = urlparse(f"{scheme.replace('_', '-').lower()}://{rest}")
    return unquote(parsed.username or ""), unquote(parsed.password or "")


def _password_from_jaas(jaas: str) -> str:
    """Extract ``user_kafka='…'`` password from Railway JAAS snippets."""
    m = re.search(r"user_kafka=['\"]([^'\"]+)['\"]", jaas)
    return m.group(1) if m else ""


def _password_from_env() -> str:
    for key, value in os.environ.items():
        if "JAAS" in key.upper() and value and "user_kafka=" in value:
            found = _password_from_jaas(value)
            if found:
                return found
    return ""


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
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    kafka_ssl_check_hostname: bool = True
    kafka_ssl_verify: bool = True
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

        bootstrap_raw = (
            _env("KAFKA_BOOTSTRAP_SERVERS")
            or _env("KAFKA_INTERNAL_URL")
            or _env("KAFKA_URL")
            or DEFAULT_KAFKA
        )
        bootstrap = _hostport_from_url(bootstrap_raw)
        url_user, url_pass = _creds_from_url(bootstrap_raw)

        username = _env("KAFKA_SASL_USERNAME") or url_user or "kafka"
        password = (
            _env("KAFKA_SASL_PASSWORD")
            or _env("KAFKA_PASSWORD")
            or url_pass
            or _password_from_env()
        )

        protocol = _env("KAFKA_SECURITY_PROTOCOL").strip().upper()
        if not protocol:
            protocol = "SASL_PLAINTEXT" if password else "PLAINTEXT"

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
            kafka_bootstrap_servers=bootstrap,
            kafka_security_protocol=protocol,
            kafka_sasl_mechanism=_env("KAFKA_SASL_MECHANISM", "PLAIN") or "PLAIN",
            kafka_sasl_username=username if password else "",
            kafka_sasl_password=password,
            kafka_ssl_check_hostname=_env_bool("KAFKA_SSL_CHECK_HOSTNAME", True),
            kafka_ssl_verify=_env_bool("KAFKA_SSL_VERIFY", True),
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
