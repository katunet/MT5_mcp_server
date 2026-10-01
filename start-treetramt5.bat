@echo off
cd /d "%~dp0"
call .venv\Scripts\activate.bat
echo [TreetraMT5] MT5 Relay starting on port 5585...
python mt5_relay.py --port 5585 --env .env.treetramt5
echo.
echo [TreetraMT5] Relay stopped. Press any key to close.
pause > nul
