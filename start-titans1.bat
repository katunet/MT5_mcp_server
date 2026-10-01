@echo off
cd /d "%~dp0"
call .venv\Scripts\activate.bat
echo [TitanS1] MT5 Relay starting on port 5582...
python mt5_relay.py --port 5582 --env .env.titans1
echo.
echo [TitanS1] Relay stopped. Press any key to close.
pause > nul
