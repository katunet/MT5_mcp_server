@echo off
setlocal enabledelayedexpansion
chcp 65001 > nul

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

echo =========================================
echo  MT5 MCP Relay - 一括起動スクリプト
echo =========================================
echo.

REM Python 仮想環境の有効化（.venv があれば）
if exist ".venv\Scripts\activate.bat" (
    call .venv\Scripts\activate.bat
    echo [OK] 仮想環境を有効化しました
) else (
    echo [INFO] 仮想環境なし。システム Python を使用します
)
echo.

REM accounts.conf の存在確認
if not exist "accounts.conf" (
    echo [エラー] accounts.conf が見つかりません。
    echo   MT5_mcp_server フォルダ内に accounts.conf を作成してください。
    pause
    exit /b 1
)

REM 起動カウンター
set STARTED=0
set SKIPPED=0

REM accounts.conf を1行ずつ読み込む
REM 形式: 口座名|ポート番号|.envファイルパス  （# 始まりはコメント）
for /f "usebackq tokens=1,2,3 delims=| eol=#" %%A in ("accounts.conf") do (
    set "ACC_NAME=%%A"
    set "ACC_PORT=%%B"
    set "ACC_ENV=%%C"

    if not "!ACC_NAME!"=="" (
        if not exist "!ACC_ENV!" (
            echo [スキップ] !ACC_NAME! : 設定ファイル "!ACC_ENV!" が見つかりません
            set /a SKIPPED+=1
        ) else (
            REM 同名のウィンドウが既に起動していないか確認（タイトルで判定）
            tasklist /fi "WINDOWTITLE eq MT5 Relay: !ACC_NAME!" 2>nul | find "cmd.exe" > nul
            if !errorlevel!==0 (
                echo [スキップ] !ACC_NAME! : すでに起動中です
                set /a SKIPPED+=1
            ) else (
                echo [起動] !ACC_NAME!  ポート: !ACC_PORT!  設定: !ACC_ENV!
                start "MT5 Relay: !ACC_NAME!" cmd /k "python mt5_relay.py --port !ACC_PORT! --env !ACC_ENV!"
                set /a STARTED+=1
                REM MT5 の初期化時間を確保するため少し待機
                timeout /t 3 /nobreak > nul
            )
        )
    )
)

echo.
echo -----------------------------------------
echo  起動: !STARTED! 口座  /  スキップ: !SKIPPED! 口座
echo -----------------------------------------
echo.

if !STARTED!==0 (
    if !SKIPPED!==0 (
        echo [警告] accounts.conf に有効な口座設定がありません。
    ) else (
        echo [警告] 起動できた口座が 0 件です。設定ファイルを確認してください。
    )
)

echo このウィンドウは閉じてかまいません。
echo 各リレーウィンドウを閉じると該当口座のリレーが停止します。
echo.
pause
endlocal
