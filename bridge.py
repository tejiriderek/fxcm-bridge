"""Read-only FXCM ForexConnect API bridge for the main scanner."""

from __future__ import annotations

import gzip
import logging
import os
import time
import threading
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from flask import Flask, jsonify

load_dotenv()

log = logging.getLogger("fxcm_bridge")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
PAIRS = {"EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "USDJPY": "USD/JPY", "EURAUD": "EUR/AUD", "NZDCAD": "NZD/CAD"}

# Cache for historical candles to avoid hammering the FXCM candledata endpoint
# key: "EURUSD|D1", value: (candle_dict_or_None, fetched_at_epoch)
_history_cache: dict = {}
_HISTORY_CACHE_TTL = {"D1": 1800, "H4": 600}  # seconds (30 min / 10 min)

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
                os.getenv("FXCM_CONNECTION", "demo")
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
                instrument_rows = df[df['instrument'] == instrument]
                if instrument_rows.empty:
                    log.warning("No data found for %s", instrument)
                    continue
                
                row = instrument_rows.iloc[-1]
                bid = float(row['bid'])
                ask = float(row['ask'])
                
                pairs[pair] = {
                    "price": (bid + ask) / 2,
                    "bid": bid,
                    "ask": ask,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "ohlc": {
                        "D1": get_history_candle(pair, 'D1'),
                        "H4": get_history_candle(pair, 'H4'),
                    },
                }
            except Exception as e:
                log.warning("Failed to get data for %s: %s", pair, e)
                continue
    except Exception as e:
        log.error("Failed to get offers table: %s", e)
    
    return {"source": "fxcm", "account_type": "demo", "sent_at": datetime.now(timezone.utc).isoformat(), "pairs": pairs}


def get_history_candle(pair: str, timeframe: str) -> dict | None:
    """
    Fetch the most recent completed OHLC candle for a pair/timeframe from FXCM's
    public candledata HTTP endpoint.

    URL format: https://candledata.fxcorporate.com/{periodicity}/{symbol}/{year}/{week}.csv.gz
    CSV columns: DateTime,BidOpen,BidHigh,BidLow,BidClose,AskOpen,AskHigh,AskLow,AskClose,Volume

    Caches results to avoid making a network request on every 30-second poll.
    Returns None if data cannot be fetched (bridge keeps working with live prices).
    """
    cache_key = f"{pair}|{timeframe}"
    now_epoch = time.time()
    
    # Check cache first
    cached = _history_cache.get(cache_key)
    if cached is not None:
        candle, fetched_at = cached
        ttl = _HISTORY_CACHE_TTL.get(timeframe, 600)
        if now_epoch - fetched_at < ttl:
            return candle
    
    try:
        periodicity = timeframe  # 'D1' and 'H4' are valid FXCM periodicities
        symbol = pair  # EURUSD, GBPUSD, etc. (no slash needed)
        
        now = datetime.now(timezone.utc)
        iso_year, iso_week, _ = now.isocalendar()
        
        # Try current week first, then previous week (useful early in the week
        # when the current week's file may not exist or may be empty yet)
        attempts = [(iso_year, iso_week)]
        prev_week = iso_week - 1
        prev_year = iso_year
        if prev_week < 1:
            prev_week = 52
            prev_year -= 1
        attempts.append((prev_year, prev_week))
        
        for year, week in attempts:
            url = f"https://candledata.fxcorporate.com/{periodicity}/{symbol}/{year}/{week}.csv.gz"
            try:
                response = requests.get(url, timeout=15)
                if response.status_code != 200:
                    log.debug("Candledata %s returned HTTP %s", url, response.status_code)
                    continue
                
                content = gzip.decompress(response.content)
                text = content.decode("utf-8", errors="ignore").strip()
                if not text:
                    continue
                
                lines = text.split("\n")
                # Skip header row if present (first field won't be a date)
                data_lines = [
                    ln for ln in lines
                    if ln and not ln.lower().startswith("datetime")
                ]
                if not data_lines:
                    continue
                
                # Last line = most recent completed candle
                last = data_lines[-1].split(",")
                if len(last) < 5:
                    continue
                
                # Use bid OHLC (columns 1-4)
                ts = last[0].strip()
                o = float(last[1])
                h = float(last[2])
                l = float(last[3])
                c = float(last[4])
                
                candle = {
                    "timestamp": ts,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "completed": True,
                }
                _history_cache[cache_key] = (candle, now_epoch)
                log.info("Fetched FXCM candle %s %s: %s", pair, timeframe, ts)
                return candle
            except Exception as inner:
                log.debug("Candledata fetch failed for %s %s week %s: %s", symbol, periodicity, week, inner)
                continue
        
        # All attempts failed — cache the None briefly so we don't retry every poll
        log.warning("No candledata available for %s %s", pair, timeframe)
        _history_cache[cache_key] = (None, now_epoch)
        return None
    except Exception as e:
        log.warning("Failed to get history for %s %s: %s", pair, timeframe, e)
        return None


if __name__ == "__main__":
    bridge_thread = threading.Thread(target=bridge_worker, daemon=True)
    bridge_thread.start()
    
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
