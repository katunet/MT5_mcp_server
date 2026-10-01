# MT5 MCP Server セットアップマニュアル

> 対象環境: **Windows ローカル PC**（MT5・リレーサーバー・Claude Code がすべて同一マシンで動作）

---

## 1. システム構成

```
Claude Code（AI）
    │  MCP プロトコル（stdio）
    ▼
server.py（MCPサーバー）
    │  ZeroMQ TCP localhost
    ├──▶ mt5_relay.py [ICMarkets] ポート 5580
    │        └── MetaTrader5 Python API ──▶ MT5端末（ICMarkets）
    │
    └──▶ mt5_relay.py [Vantage]   ポート 5581
             └── MetaTrader5 Python API ──▶ MT5端末（Vantage）
```

- **server.py**: Claude Code に MT5 データを MCP ツールとして公開する
- **mt5_relay.py**: MT5 Python API を叩いてデータを返すリレーサーバー。口座ごとに1プロセス起動する
- **ZeroMQ**: server.py と mt5_relay.py 間の通信（localhost 内で完結）

---

## 2. 前提条件

| 必要なもの | 備考 |
|------------|------|
| Python 3.10 以上 | `python --version` で確認 |
| MetaTrader 5 端末 | 各ブローカーからインストール |
| Claude Code | MCP サーバーとして server.py を登録する |

---

## 3. Python 環境セットアップ

```powershell
cd C:\Users\katun\EA開発\MT5_mcp_server

# 仮想環境の作成（初回のみ）
python -m venv .venv

# 仮想環境の有効化
.venv\Scripts\activate

# 依存パッケージのインストール（初回のみ）
pip install -r requirements-vps.txt   # mt5_relay.py 用（MetaTrader5, pyzmq）
pip install -r requirements.txt       # server.py 用（mcp, pyzmq, python-dotenv）
```

> `(venv)` がプロンプトに表示されれば有効化できています。

---

## 4. 設定ファイルの構成

```
MT5_mcp_server/
├── .env                    ← server.py が読む共通設定（MT5_ACCOUNTS など）
├── .env.icmarkets          ← ICMarkets リレー用（ログイン情報）
├── .env.vantage            ← Vantage リレー用（ログイン情報）
├── accounts.conf           ← 口座リスト（起動スクリプトが参照）
└── start-relays.bat        ← 全リレーの一括起動スクリプト
```

### 4-1. `.env`（共通設定）

```ini
MT5_ZMQ_TIMEOUT=30000
MT5_TICK_CACHE=C:\Users\katun\MQ\tick

# 口座名:localhost:ポート をカンマ区切りで列挙
MT5_ACCOUNTS=ICMarkets:localhost:5580,Vantage:localhost:5581
```

> `MT5_ACCOUNTS` の口座名は Claude Code から `account` パラメータで指定するときの名前になる。

### 4-2. 口座ごとの `.env.<口座名>`

```ini
# .env.vantage の例
MT5_ZMQ_PORT=5581
MT5_LOGIN=12345678
MT5_PASSWORD=パスワード
MT5_SERVER=VantageTradingLtd-Live
# MT5_TERMINAL_PATH=C:\Program Files\MetaTrader 5 Vantage  ← 複数MT5を使い分ける場合
```

### 4-3. `accounts.conf`（口座リスト）

```
# 形式: 口座名|ポート番号|.envファイル名
ICMarkets|5580|.env.icmarkets
Vantage|5581|.env.vantage
```

---

## 5. 新しい口座を追加する手順

### ステップ 1 — 口座設定ファイルを作成

```powershell
copy .env.account.example .env.titanfx
```

`.env.titanfx` をテキストエディタで開き、ログイン情報を記入する：

```ini
MT5_ZMQ_PORT=5582          ← 既存口座と重複しないポート番号
MT5_LOGIN=ログイン番号
MT5_PASSWORD=パスワード
MT5_SERVER=サーバー名
```

### ステップ 2 — `accounts.conf` に追記

```
ICMarkets|5580|.env.icmarkets
Vantage|5581|.env.vantage
TitanFX|5582|.env.titanfx   ← 追加
```

### ステップ 3 — `.env` の `MT5_ACCOUNTS` に追記

```ini
MT5_ACCOUNTS=ICMarkets:localhost:5580,Vantage:localhost:5581,TitanFX:localhost:5582
```

### ステップ 4 — 再起動

`start-relays.bat` を実行すれば新しい口座のリレーも自動起動する。

---

## 6. リレーサーバーの起動

### 通常起動（推奨）

`start-relays.bat` をダブルクリック、または PowerShell から：

```powershell
.\start-relays.bat
```

`accounts.conf` に書かれた口座分だけ、それぞれ別ウィンドウでリレーが起動する。

### 手動で1口座だけ起動する場合

```powershell
.venv\Scripts\activate
python mt5_relay.py --port 5581 --env .env.vantage
```

起動成功時のログ：

```
2026-04-12 12:31:01 [INFO] Loaded env: .env.vantage
2026-04-12 12:31:01 [INFO] MT5 connected: account=12345678, server=VantageTradingLtd-Live
2026-04-12 12:31:01 [INFO] ZMQ REP listening on tcp://127.0.0.1:5581
```

> **注意**: `--port` と `--env` を省略すると `.env`（共通設定）を読み込み、すでに起動中の MT5 端末に自動接続する。複数口座を使う場合は必ず引数を指定すること。

---

## 7. ティックキャッシュ

一括ティックダウンロード（`download_ticks_bulk` ツール）の保存先：

```
C:\Users\katun\MQ\tick\
└── <ブローカー名>\
    └── <シンボル>\
        └── ticks\
            ├── 2026-01-01.csv
            ├── 2026-01-02.csv
            └── ...
```

保存先を変更したい場合は `.env` の `MT5_TICK_CACHE` を編集する。

---

## 8. Claude Code への MCP 登録

Claude Code の MCP 設定（`.claude/settings.json` または `claude_desktop_config.json`）に以下を追加する：

```json
{
  "mcpServers": {
    "mt5": {
      "command": "C:\\Users\\katun\\EA開発\\MT5_mcp_server\\.venv\\Scripts\\python.exe",
      "args": ["C:\\Users\\katun\\EA開発\\MT5_mcp_server\\server.py"]
    }
  }
}
```

登録後、Claude Code を再起動すると MT5 ツールが使えるようになる。

---

## 9. 利用可能な MCP ツール一覧

| ツール名 | 説明 |
|----------|------|
| `list_accounts` | 設定済み口座の一覧を取得 |
| `get_account_info` | 残高・証拠金・損益等の口座情報 |
| `get_positions` | オープンポジション一覧 |
| `get_ohlcv` | ローソク足データ（M1〜MN1） |
| `get_ticks` | ティックデータ（Bid/Ask） |
| `get_symbol_info` | スプレッド・スワップ等の取引条件 |
| `get_history_deals` | 約定履歴（期間指定） |
| `get_history_orders` | 注文履歴（期間指定） |
| `download_ticks_bulk` | 指定期間のティックを CSV に一括保存 |

複数口座がある場合、各ツールに `account` パラメータで口座名を指定できる：

```
get_account_info(account="Vantage")
get_ohlcv(symbol="XAUUSD", timeframe="H1", count=1000, account="ICMarkets")
```

---

## 10. トラブルシューティング

### `ModuleNotFoundError: No module named 'zmq'`

```powershell
pip install -r requirements-vps.txt
```

### `ModuleNotFoundError: No module named 'dotenv'`

```powershell
pip install python-dotenv
```

### `MT5 initialize failed`

- MT5 端末が起動しているか確認
- `.env.<口座名>` の `MT5_LOGIN` / `MT5_PASSWORD` / `MT5_SERVER` が正しいか確認
- MT5 端末が同じ口座でログイン済みの場合はログイン情報なしでも接続できる

### ポートの競合（`Address already in use`）

- 同じポートで別のリレーがすでに起動していないか確認
- タスクマネージャーで `python.exe` プロセスを終了してから再起動

### `start-relays.bat` で口座がスキップされる

- `.env.<口座名>` ファイルが存在するか確認
- `accounts.conf` のファイルパスが正しいか確認（スペースに注意）

---

## 8. バックテスト機能のセットアップ

`run_backtest` / `get_backtest_status` / `get_backtest_result` ツールを使用するには、
各口座の `.env` ファイルに以下を追加する。

### 8-1. ポータブルモードの設定（重要）

ほとんどの口座が `/portable` で動いている場合、**各口座の `.env`** に以下を追記する：

```ini
MT5_PORTABLE=true
```

これにより `metatester64.exe` 起動時に `/portable` フラグが自動付与される。
設定しない場合、MT5 が標準インストールのデータフォルダを参照してしまい、EA が見つからない。

### 8-2. 各口座の `.env` への追記（口座ごとに設定）

| 口座 | ファイル | 追記内容 |
|------|---------|---------|
| Vantage | `.env.vantage` | `MT5_TESTER_PATH=C:\Users\katun\MQ\Vantage1\metatester64.exe` |
| ICMarkets | `.env.icmarkets` | `MT5_TESTER_PATH=C:\Users\katun\MQ\<ICMarketsフォルダ>\metatester64.exe` |
| TitanS1 | `.env.titan` | `MT5_TESTER_PATH=C:\Users\katun\MQ\TitanS1\metatester64.exe` |

> `MT5_TERMINAL_PATH` が設定済みの場合、同じフォルダの `metatester64.exe` を自動推測するため省略可能。

### 8-3. 口座の切り替え方

`run_backtest` の `account` パラメーターに口座名を指定するだけ：

```
account: Vantage     ← Vantage口座のリレー（ポート5581）でバックテスト
account: ICMarkets   ← ICMarkets口座のリレー（ポート5580）でバックテスト
```

各口座のリレーが動いていれば（`start-relays.bat` で起動済み）、それぞれのMT5インスタンスでバックテストが実行される。

### 8-4. バックテストの実行フロー

```
1. run_backtest        → 非同期でバックテスト開始（即座に "started" を返す）
2. get_backtest_status → ポーリングで完了を確認（"running" / "completed" / "failed"）
3. get_backtest_result → 完了後に主要指標（PF・DD・勝率等）を取得
```

### 8-5. 使用例（Claude Code からの呼び出し）

```
run_backtest:
  expert: "Advisors\AUDCADZone_EA"
  symbol: AUDCAD
  period: H1
  from_date: "2024.01.01"
  to_date: "2026.04.01"
  deposit: 10000
  leverage: 100
  model: 1          # 1=Open prices only, 0=Every tick
  portable: true    # ポータブルモード（.env の MT5_PORTABLE=true でも可）
  account: Vantage  # 口座切り替えはここで指定
  inputs:
    InpLot: "0.1"
    InpTpPips: "7.0"
    InpSlPips: "50.0"
```

### 8-4. 注意事項

- 同時に実行できるバックテストは **1つ**（複数実行は順番待ち）
- `ShutdownTerminal=1` を設定しているため、バックテスト完了後にMT5ターミナルは自動終了する
- `get_backtest_result` が取得する結果は `.xml` → `.htm` の優先順で読み込む
