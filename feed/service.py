"""Fetch BTC 1h bars, publish hourly + daily series inside RabbitMQ event."""

from __future__ import annotations

import logging
import time

from feed.bars_codec import bars_to_records, resample_daily, select_publish_columns
from feed.config import FeedConfig
from feed.market import MarketFeed, latest_closed_bar_time
from strats_sdk.rabbit import BarsFetchedEvent, RabbitBus, publish

logger = logging.getLogger(__name__)


class BarFetcherService:
    def __init__(self, cfg: FeedConfig, *, feed: MarketFeed | None = None, bus: RabbitBus | None = None) -> None:
        self.cfg = cfg
        self.feed = feed or MarketFeed(cfg.cache_dir)
        self.bus = bus or RabbitBus(cfg.rabbitmq_url)
        self._producer = self.bus.producer()
        self.topic = cfg.bars_topic
        self._last_daily_fingerprint = ""

    def tick(self) -> BarsFetchedEvent | None:
        request = self.cfg.to_data_request()
        snapshot = self.feed.fetch(request)
        if not self.cfg.publish_on_unchanged and not self.feed.has_new_data(snapshot):
            logger.info("No new closed 1h bar (%s); skip publish", snapshot.fingerprint())
            return None

        closed = snapshot.last_closed_bar
        if closed is None:
            logger.warning("No closed 1h bar; skip publish")
            return None

        lean = select_publish_columns(snapshot.bars)

        daily = resample_daily(lean)
        closed_daily = latest_closed_bar_time(daily, "1D")
        daily_fp = closed_daily.isoformat() if closed_daily is not None else ""
        if not daily_fp:
            new_daily = False
        elif not self._last_daily_fingerprint:
            # First tick after process start — remember day, don't force daily strats.
            self._last_daily_fingerprint = daily_fp
            new_daily = False
        else:
            new_daily = daily_fp != self._last_daily_fingerprint
            self._last_daily_fingerprint = daily_fp

        bars_1h = bars_to_records(lean, limit=self.cfg.publish_bar_count)
        bars_1d = bars_to_records(daily, limit=self.cfg.publish_daily_bar_count)

        event = BarsFetchedEvent(
            symbol=request.symbol,
            timeframe=request.timeframe,
            market=request.market,
            last_closed_bar=closed.isoformat(),
            fetched_at=snapshot.fetched_at.isoformat(),
            lookback_days=request.lookback_days,
            bars=bars_1h,
            bars_daily=bars_1d,
            last_closed_daily_bar=daily_fp,
            new_daily_bar=new_daily,
            with_metrics=request.with_metrics,
            with_funding=request.with_funding,
            bar_count=len(bars_1h),
            daily_bar_count=len(bars_1d),
        )
        publish(self._producer, self.topic, event.to_json(), key=f"{event.symbol}:{event.timeframe}")
        self._producer.flush(timeout=10)
        logger.info(
            "Published bars.fetched %s 1h=%s (n=%d) daily=%s new_daily=%s (n=%d)",
            event.symbol,
            event.last_closed_bar,
            event.bar_count or 0,
            event.last_closed_daily_bar or "-",
            event.new_daily_bar,
            event.daily_bar_count or 0,
        )
        return event

    def run_loop(self) -> None:
        logger.info(
            "Feed started %s %s poll=%ss publish_1h=%d publish_1d=%d rabbitmq=%s",
            self.cfg.symbol,
            self.cfg.timeframe,
            self.cfg.poll_seconds,
            self.cfg.publish_bar_count,
            self.cfg.publish_daily_bar_count,
            self.cfg.rabbitmq_url,
        )
        try:
            while True:
                try:
                    self.tick()
                except Exception:
                    logger.exception("Feed tick failed")
                if self.cfg.run_once:
                    break
                time.sleep(self.cfg.poll_seconds)
        finally:
            self._producer.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    BarFetcherService(FeedConfig.from_env()).run_loop()
