# FXCM Bridge

This bridge runs separately from the Render scanner because the official ForexConnect Python SDK uses a legacy/native runtime.

## Requirements

- Windows or a compatible ForexConnect host
- Python 3.7 recommended by the official ForexConnect package
- A rotated FXCM demo password
- `forexconnect` installed in this bridge environment

```powershell
py -3.7 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install forexconnect requests python-dotenv
```

## Environment

Create `.env` in this folder. Never commit it.

```env
FXCM_ENABLED=true
FXCM_USERNAME=
FXCM_PASSWORD=
FXCM_CONNECTION=demo
FXCM_SERVER=https://www.fxcorporate.com/Hosts.jsp
FXCM_POLL_SECONDS=30
SCANNER_RECEIVER_URL=https://battousai-bot.onrender.com/fxcm/market-data
FXCM_BRIDGE_SHARED_SECRET=
```

The bridge sends only normalized market data. It never sends orders and never controls the scanner strategy.

## Render deployment

The repository Blueprint defines this directory as a separate Render background worker using Python 3.7.10. Set the three prompted secrets in the `fxcm-bridge` worker: `FXCM_USERNAME`, `FXCM_PASSWORD`, and `FXCM_BRIDGE_SHARED_SECRET`. The shared secret must exactly match the `FXCM_BRIDGE_SHARED_SECRET` value on the `battoujutsu-bot` web service. The worker requires a paid Render worker plan; it cannot run continuously on the free web-service plan.
