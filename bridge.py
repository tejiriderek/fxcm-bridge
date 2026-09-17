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

# Flask app for health checks
app = Flask(__name__)
bridge_running = False
bridge_thread = None


@app.route("/health")
def health():
    """Health check endpoint for UptimeRobot."""
    return jsonify({"status": "ok", "bridge_running": bridge_running})


def bridge_worker():
    """Run the FXCM bridge in a background thread."""
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

    from forexconnect import ForexConnect, SessionStatusListener, ResponseListener

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
        # Get the Offers table using TableManager
        table_manager = session.table_manager
        offers_table = table_manager.get_table("Offers")
        
        # Convert to pandas DataFrame for easier manipulation
        from forexconnect.common import Common
        df = Common.convert_table_to_dataframe(offers_table)
        
        for pair, instrument in PAIRS.items():
            try:
                # Find the row for this instrument
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
                        "D1": get_history_candle(session, instrument, 'D1'),
                        "H4": get_history_candle(session, instrument, 'H4'),
                    },
                }
            except Exception as e:
                log.warning("Failed to get data for %s: %s", pair, e)
                continue
    except Exception as e:
        log.error("Failed to get offers table: %s", e)
    
    return {"source": "fxcm", "account_type": "demo", "sent_at": datetime.now(timezone.utc).isoformat(), "pairs": pairs}


def get_history_candle(session, instrument: str, timeframe: str) -> dict | None:
    try:
        # Get historical data using LiveHistory
        from forexconnect import LiveHistory, LiveHistoryCreator
        history = LiveHistoryCreator.create(session)
        
        # Map timeframe to ForexConnect period
        period_map = {'D1': 'D1', 'H4': 'H4'}
        period = period_map.get(timeframe, 'D1')
        
        # Get historical candles
        candles = history.get_history(instrument, period, 1)
        if candles is None or len(candles) == 0:
            return None
        
        candle = candles[-1]
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "open": float(candle['open']),
            "high": float(candle['high']),
            "low": float(candle['low']),
            "close": float(candle['close']),
            "completed": True,
        }
    except Exception as e:
        log.warning("Failed to get history for %s %s: %s", instrument, timeframe, e)
        return None


if __name__ == "__main__":
    # Start bridge worker in background thread
    bridge_thread = threading.Thread(target=bridge_worker, daemon=True)
    bridge_thread.start()
    
    # Start Flask server (Render provides PORT env var)
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
