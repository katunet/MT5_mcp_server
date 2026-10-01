@echo off
cd /d "%~dp0"
call .venv\Scripts\activate.bat
echo [HFMKatana] MT5 Relay starting on port 5583...
python mt5_relay.py --port 5583 --env .env.hfmkatana
echo.
echo [HFMKatana] Relay stopped. Press any key to close.
pause > nul
