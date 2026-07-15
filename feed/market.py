"""Fetch bars via strats-sdk and persist to parquet cache."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from strats_sdk import (
    CcxtParams,
    FundingRatesParams,
    FuturesKlinesParams,
    ensure_utc_index,
    fetch_funding_rates,
    fetch_futures_klines,
    fetch_metrics,
    fetch_spot,
)
from strats_sdk.binance.metrics import MetricsParams
from strats_sdk.platform import DataRequest, MarketSnapshot

logger = logging.getLogger(__name__)


class MarketFeed:
    def __init__(self, cache_dir: Path) -> None:
        self._last_fingerprint = ""
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch(self, request: DataRequest) -> MarketSnapshot:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=request.lookback_days)

        start_s = start.isoformat()
        end_s = end.isoformat()

        logger.info(
            "Fetching %s %s %s (%s → %s)",
            request.market,
            request.symbol,
            request.timeframe,
            start_s,
            end_s,
        )

        funding_df: pd.DataFrame | None = None
        metrics_df: pd.DataFrame | None = None

        if request.market == "spot":
            bars = fetch_spot(
                CcxtParams(
                    exchange_id="binance",
                    symbol=_to_ccxt_symbol(request.symbol),
                    timeframe=request.timeframe,
                    start=start_s,
                    end=end_s,
                    cache_dir=self.cache_dir,
                )
            )
            bars = ensure_utc_index(bars)
        else:
            bars = fetch_futures_klines(
                FuturesKlinesParams(
                    symbol=request.symbol,
                    interval=request.timeframe,
                    start=start_s,
                    end=end_s,
                )
            )
            bars = ensure_utc_index(bars)

            if request.with_funding:
                funding_df = fetch_funding_rates(
                    FundingRatesParams(symbol=request.symbol, start=start_s, end=end_s)
                )
                bars = merge_funding(bars, funding_df)

            if request.with_metrics:
                metrics_df = fetch_metrics(
                    MetricsParams(symbol=request.symbol, start=start_s, end=end_s),
                    resample="1h",
                )
                bars = merge_metrics_hourly(bars, metrics_df) if metrics_df is not None else bars

        closed = latest_closed_bar_time(bars, request.timeframe)
        return MarketSnapshot(
            request=request,
            fetched_at=end,
            bars=bars,
            funding=funding_df,
            metrics=metrics_df,
            last_closed_bar=closed,
        )

    def has_new_data(self, snapshot: MarketSnapshot) -> bool:
        fp = snapshot.fingerprint()
        if not fp or fp == self._last_fingerprint:
            return False
        self._last_fingerprint = fp
        return True


def latest_closed_bar_time(bars: pd.DataFrame, timeframe: str) -> pd.Timestamp | None:
    bar_delta = pd.Timedelta(timeframe)
    now = pd.Timestamp.now(tz="UTC")
    closed = bars[bars.index + bar_delta <= now]
    if closed.empty:
        return None
    return closed.index[-1]


def _to_ccxt_symbol(symbol: str) -> str:
    return symbol.replace("USDT", "/USDT") if "/" not in symbol else symbol


def merge_funding(bars: pd.DataFrame, funding: pd.DataFrame) -> pd.DataFrame:
    if funding.empty:
        return bars
    out = bars.join(funding, how="left")
    if "funding_rate" in out.columns:
        out["funding_rate"] = out["funding_rate"].ffill()
    return out


def merge_metrics_hourly(bars: pd.DataFrame, metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics is None or metrics.empty:
        return bars
    aligned = metrics.reindex(bars.index, method="ffill")
    for col in aligned.columns:
        if col not in bars.columns:
            bars[col] = aligned[col]
    return bars
