"""Serialize / deserialize OHLCV bars for Kafka payloads."""

from __future__ import annotations

from typing import Any

import pandas as pd

# Keep Kafka payloads lean: OHLCV + a few enrichments only.
DEFAULT_PUBLISH_COLUMNS = (
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "funding_rate",
    "sum_open_interest",
    "count_toptrader_long_short_ratio",
)


def select_publish_columns(bars: pd.DataFrame, columns: tuple[str, ...] = DEFAULT_PUBLISH_COLUMNS) -> pd.DataFrame:
    keep = [c for c in columns if c in bars.columns]
    if not keep:
        raise ValueError(f"No publish columns found in bars; have {list(bars.columns)}")
    return bars.loc[:, keep].copy()


def resample_daily(bars: pd.DataFrame) -> pd.DataFrame:
    """UTC daily bars from 1h (or finer) OHLCV."""
    df = bars.sort_index()
    agg: dict[str, str] = {}
    if "Open" in df.columns:
        agg["Open"] = "first"
    if "High" in df.columns:
        agg["High"] = "max"
    if "Low" in df.columns:
        agg["Low"] = "min"
    if "Close" in df.columns:
        agg["Close"] = "last"
    if "Volume" in df.columns:
        agg["Volume"] = "sum"
    for col in df.columns:
        if col in agg:
            continue
        # last value of extra metrics/funding within the day
        agg[col] = "last"
    daily = df.resample("1D").agg(agg).dropna(subset=[c for c in ("Open", "Close") if c in agg])
    return daily


def bars_to_records(bars: pd.DataFrame, *, limit: int | None = None) -> list[dict[str, Any]]:
    """Convert bar DataFrame → JSON-safe records (newest ``limit`` rows if set)."""
    df = bars.sort_index()
    if limit is not None and limit > 0 and len(df) > limit:
        df = df.iloc[-limit:]
    out: list[dict[str, Any]] = []
    for ts, row in df.iterrows():
        rec: dict[str, Any] = {"time": pd.Timestamp(ts).tz_convert("UTC").isoformat()}
        for col, val in row.items():
            if pd.isna(val):
                continue
            if hasattr(val, "item"):
                val = val.item()
            rec[str(col)] = val
        out.append(rec)
    return out


def records_to_bars(records: list[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame.from_records(records)
    if "time" not in df.columns:
        raise ValueError("bar records missing 'time'")
    df["time"] = pd.to_datetime(df["time"], utc=True)
    return df.set_index("time").sort_index()
