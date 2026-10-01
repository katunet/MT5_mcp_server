@echo off
cd /d "%~dp0"
call .venv\Scripts\activate.bat
echo [XSMT5pro] MT5 Relay starting on port 5584...
python mt5_relay.py --port 5584 --env .env.xsmt5pro
echo.
echo [XSMT5pro] Relay stopped. Press any key to close.
pause > nul
