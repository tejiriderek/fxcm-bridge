"""Read-only FXCM ForexConnect API bridge for the main scanner."""

from __future__ import annotations

import logging
import os
import time
import threading
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify

load_dotenv()

log = logging.getLogger("fxcm_bridge")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
PAIRS = {"EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "USDJPY": "USD/JPY", "EURAUD": "EUR/AUD", "NZDCAD": "NZD/CAD"}

_history_cache: dict = {}
_HISTORY_CACHE_TTL = {"D1": 1800, "H4": 600}

app = Flask(__name__)
bridge_running = False
bridge_thread = None


@app.route("/health")
def health():
    return jsonify({"status": "ok", "bridge_running": bridge_running})


def bridge_worker():
    global bridge_running
    bridge_running = True
    try:
        main()
    except Exception as e:
        log.error("Bridge worker failed: %s", e)
        bridge_running = False


def main() -> None:
    if os.getenv("FXCM_ENABLED", "false").lower() not in {"true", "1", "yes"}:
        raise RuntimeError("FXCM_ENABLED is not true")

    username = os.getenv("FXCM_USERNAME", "")
    password = os.getenv("FXCM_PASSWORD", "")
    receiver = os.getenv("SCANNER_RECEIVER_URL", "").rstrip("/")
    secret = os.getenv("FXCM_BRIDGE_SHARED_SECRET", "")

    if not username or not password or not receiver or not secret:
        raise RuntimeError("FXCM_USERNAME, FXCM_PASSWORD, SCANNER_RECEIVER_URL, and FXCM_BRIDGE_SHARED_SECRET are required")

    from forexconnect import ForexConnect

    while True:
        try:
            session = ForexConnect()
            log.info("Connecting to FXCM ForexConnect API...")
            session.login(
                username,
                password,
                "https://www.fxcorporate.com/Hosts.jsp",
                os.getenv("FXCM_CONNECTION", "demo"),
            )
            log.info("FXCM connected via ForexConnect API")

            while True:
                payload = collect_snapshot(session)
                response = requests.post(
                    receiver,
                    json=payload,
                    headers={"X-FXCM-Bridge-Secret": secret},
                    timeout=10,
                )
                response.raise_for_status()
                time.sleep(max(5, int(os.getenv("FXCM_POLL_SECONDS", "30"))))
        except Exception as exc:
            log.warning("FXCM bridge disconnected: %s; retrying", type(exc).__name__)
            time.sleep(10)


def collect_snapshot(session) -> dict:
    pairs = {}
    try:
        from forexconnect import ForexConnect
        table_manager = session.table_manager
        offers_table = table_manager.get_table(ForexConnect.OFFERS)

        from forexconnect.common import Common
        df = Common.convert_table_to_dataframe(offers_table)

        for pair, instrument in PAIRS.items():
            try:
                instrument_rows = df[df["instrument"] == instrument]
                if instrument_rows.empty:
                    log.warning("No data found for %s", instrument)
                    continue

                row = instrument_rows.iloc[-1]
                bid = float(row["bid"])
                ask = float(row["ask"])

                pairs[pair] = {
                    "price": (bid + ask) / 2,
                    "bid": bid,
                    "ask": ask,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "ohlc": {
                        "D1": get_history_candle(session, instrument, "D1"),
                        "H4": get_history_candle(session, instrument, "H4"),
                    },
                }
            except Exception as e:
                log.warning("Failed to get data for %s: %s", pair, e)
                continue
    except Exception as e:
        log.error("Failed to get offers table: %s", e)

    return {
        "source": "fxcm",
        "account_type": "demo",
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "pairs": pairs,
    }


def get_history_candle(session, instrument: str, timeframe: str) -> dict | None:
    """
    Fetch the most recent completed OHLC candle via ForexConnect's
    native get_history method.

    FXCM's Python API returns different shapes depending on version:
      - pandas DataFrame with columns Date, BidOpen, BidHigh, BidLow, BidClose, ...
      - list of dicts with the same keys
      - list of tuples (Date, BidOpen, BidHigh, BidLow, BidClose, ...)

    We normalize to pandas DataFrame, resolve column names, and fall back
    to positional access if named columns aren't available.
    """
    cache_key = f"{instrument}|{timeframe}"
    now_epoch = time.time()

    cached = _history_cache.get(cache_key)
    if cached is not None:
        candle, fetched_at = cached
        ttl = _HISTORY_CACHE_TTL.get(timeframe, 600)
        if now_epoch - fetched_at < ttl:
            return candle

    try:
        now = datetime.utcnow()
        now_utc = datetime.now(timezone.utc)
        if timeframe == "D1":
            start = now - timedelta(days=10)
        else:
            start = now - timedelta(days=3)

        try:
            history = session.get_history(instrument, timeframe, start, now)
        except AttributeError:
            log.error("session.get_history not available; cannot fetch history")
            _history_cache[cache_key] = (None, now_epoch)
            return None

        if history is None:
            log.warning("No history returned for %s %s", instrument, timeframe)
            _history_cache[cache_key] = (None, now_epoch)
            return None

        # === DIAGNOSTIC: log type and a short repr once per pair/timeframe ===
        diag_key = f"{cache_key}|diag_logged"
        if not _history_cache.get(diag_key):
            try:
                log.info("get_history type=%s repr=%s",
                         type(history).__name__, repr(history)[:400])
            except Exception:
                pass
            _history_cache[diag_key] = True

        # Normalize to pandas DataFrame
        import pandas as pd
        df = None
        try:
            if isinstance(history, pd.DataFrame):
                df = history
            else:
                df = pd.DataFrame(history)
        except Exception as conv_e:
            log.warning("Could not convert history to DataFrame for %s %s: %s",
                        instrument, timeframe, conv_e)

        if df is None or df.empty:
            log.warning("Empty history for %s %s", instrument, timeframe)
            _history_cache[cache_key] = (None, now_epoch)
            return None

        # Log columns once so we know what we're dealing with
        col_diag_key = f"{cache_key}|cols_logged"
        if not _history_cache.get(col_diag_key):
            try:
                log.info("history columns for %s %s: %s",
                         instrument, timeframe, list(df.columns))
            except Exception:
                pass
            _history_cache[col_diag_key] = True

        interval = timedelta(days=1 if timeframe == "D1" else 4 / 24)

        def raw_timestamp(candidate):
            for name in ("Date", "date", "Time", "time", "Datetime", "datetime"):
                if name in df.columns and candidate[name] is not None:
                    return candidate[name]
            values = list(candidate.values) if hasattr(candidate, "values") else list(candidate)
            return values[0] if values else None

        completed_rows = []
        for _, candidate in df.iterrows():
            raw = raw_timestamp(candidate)
            if raw is None:
                continue
            try:
                parsed = pd.Timestamp(raw)
                if parsed.tzinfo is None:
                    parsed = parsed.tz_localize("UTC")
                parsed = parsed.tz_convert("UTC").to_pydatetime()
            except (TypeError, ValueError):
                continue
            if parsed + interval <= now_utc:
                completed_rows.append((parsed, candidate))
        if not completed_rows:
            log.warning("No completed %s candle returned for %s", timeframe, instrument)
            _history_cache[cache_key] = (None, now_epoch)
            return None

        candle_timestamp, row = sorted(completed_rows, key=lambda item: item[0])[-1]

        def col(*names):
            for n in names:
                try:
                    if n in df.columns:
                        v = row[n]
                        if v is not None:
                            return v
                except Exception:
                    continue
            return None

        ts = col("Date", "date", "Time", "time", "Datetime", "datetime")
        o = col("BidOpen", "Open", "open")
        h = col("BidHigh", "High", "high")
        l = col("BidLow", "Low", "low")
        c = col("BidClose", "Close", "close")

        # Positional fallback (common FXCM order: Date, BidOpen, BidHigh, BidLow, BidClose, ...)
        if o is None or h is None or l is None or c is None:
            try:
                vals = list(row.values) if hasattr(row, "values") else list(row)
                if len(vals) >= 5:
                    ts = ts if ts is not None else vals[0]
                    o = o if o is not None else vals[1]
                    h = h if h is not None else vals[2]
                    l = l if l is not None else vals[3]
                    c = c if c is not None else vals[4]
            except Exception as pos_e:
                log.warning("Positional fallback failed for %s %s: %s",
                            instrument, timeframe, pos_e)

        if o is None or h is None or l is None or c is None:
            log.warning("Missing OHLC fields for %s %s. First row repr: %s",
                        instrument, timeframe, repr(row)[:400])
            _history_cache[cache_key] = (None, now_epoch)
            return None

        result = {
            "timestamp": candle_timestamp.isoformat(),
            "open": float(o),
            "high": float(h),
            "low": float(l),
            "close": float(c),
            "completed": True,
        }
        _history_cache[cache_key] = (result, now_epoch)
        log.info("Fetched FXCM candle %s %s: ts=%s O=%s H=%s L=%s C=%s",
                 instrument, timeframe, ts, o, h, l, c)
        return result

    except Exception as e:
        log.warning("Failed to get history for %s %s: %s: %s",
                    instrument, timeframe, type(e).__name__, e)
        _history_cache[cache_key] = (None, now_epoch)
        return None


if __name__ == "__main__":
    bridge_thread = threading.Thread(target=bridge_worker, daemon=True)
    bridge_thread.start()

    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
