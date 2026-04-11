# MT5 MCPサーバー設計書

## 目的

VPS上のMT5ターミナルからOHLCV・ティック・口座情報等を取得するMCPサーバーを構築し、Mac上でのEA開発時にClaudeが高精度なバックテストを行えるようにする。

## アーキテクチャ

```
┌─ Mac (開発PC) ──────────────────────┐          ┌─ VPS (Windows) ─────────────────────┐
│                                      │          │                                     │
│  Claude Code                         │          │  mt5_relay.py (常駐プロセス)          │
│    ↓ MCP stdio                       │          │  ├─ ZMQ REP socket :5580            │
│  MT5_mcp/server.py                   │  SSH     │  ├─ MetaTrader5 Python API          │
│  ├─ pyzmq REQ ──────────────────────────tunnel──── │  └─ JSON シリアライズ              │
│  ├─ MCP tools 定義                   │  :5580   │          ↓                           │
│  └─ レスポンス → Claude に返却       │          │  MT5 ターミナル                      │
└──────────────────────────────────────┘          └─────────────────────────────────────┘
```

### 通信方式

- **プロトコル**: ZeroMQ REQ/REP over SSHトンネル
- **ポート**: 5580 (EA_Copierの5555/5556と衝突しない)
- **メッセージ形式**: JSON

### メッセージフォーマット

リクエスト:
```json
{"command": "get_ohlcv", "params": {"symbol": "XAUUSD", "timeframe": "H1", "count": 1000}}
```

レスポンス (成功):
```json
{"status": "ok", "data": [...]}
```

レスポンス (エラー):
```json
{"status": "error", "code": 10004, "message": "MT5 request failed"}
```

## MCPツール定義

| ツール名 | 用途 | パラメータ |
|---------|------|-----------|
| `get_account_info` | 口座情報（残高・証拠金・損益） | なし |
| `get_positions` | オープンポジション一覧 | `symbol`(任意) |
| `get_ohlcv` | ローソク足データ | `symbol`, `timeframe`, `count`, `start_time`(任意) |
| `get_ticks` | ティックデータ（Bid/Ask履歴） | `symbol`, `count`, `start_time`(任意) |
| `get_symbol_info` | シンボル詳細（スプレッド・スワップ・取引条件） | `symbol` |
| `get_history_deals` | 約定履歴 | `start_time`, `end_time`, `symbol`(任意) |
| `get_history_orders` | 注文履歴 | `start_time`, `end_time`, `symbol`(任意) |

### timeframe 指定

MT5定数名を文字列で指定: `M1`, `M5`, `M15`, `M30`, `H1`, `H4`, `D1`, `W1`, `MN1`

### 取得件数上限

- OHLCV: 最大約100,000本/リクエスト
- ティック: 最大約1,000,000ティック/リクエスト (メモリ依存)
- 大量データは分割リクエストをMCPサーバー側で処理

## コンポーネント詳細

### Mac側: MCPサーバー (`MT5_mcp/server.py`)

```
MT5_mcp/
├─ server.py          # MCPサーバー本体（ツール定義 + ZMQクライアント）
├─ requirements.txt   # mcp, pyzmq, python-dotenv
├─ .env.example       # 設定テンプレート
└─ .env               # 接続設定
```

**接続設定 (.env)**:
```
MT5_ZMQ_HOST=localhost
MT5_ZMQ_PORT=5580
MT5_ZMQ_TIMEOUT=30000
```

**ZMQクライアントの挙動**:
- 起動時にZMQ REQソケットを作成、connect()
- ツール呼び出しごとにリクエスト送信 → レスポンス待ち
- タイムアウト時はソケットを再作成して再接続（ZMQ REQソケットはタイムアウト後に状態が壊れるため）

**MCPプロトコル**: stdio (Claude Codeのsettings.jsonに登録)

```json
{
  "mcpServers": {
    "mt5": {
      "command": "python3",
      "args": ["server.py"],
      "cwd": "/Users/katunet00/EA開発等/MT5_mcp"
    }
  }
}
```

### VPS側: リレーサーバー (`mt5_relay.py`)

**単一ファイル構成** (EA_CopierのMiddlewareと同じパターン):
- MT5接続管理 (initialize / shutdown)
- コマンドディスパッチャー (command → handler マッピング)
- 各コマンドハンドラー
- ZMQ REPソケット (メインループ)
- データ変換 (numpy → JSON)

**MT5接続管理**:
- 起動時に `mt5.initialize()`、終了時に `mt5.shutdown()`
- リクエストごとの再接続はしない
- 接続切れ検知時のみ再接続

**データシリアライズ**:
- numpy構造体 → Python dict → `json.dumps()`
- 日時は ISO 8601 文字列
- float精度は元データのまま保持

**バインドアドレス**: `tcp://127.0.0.1:5580` (localhost限定)

## セキュリティ

| レイヤー | 対策 |
|---------|------|
| 通信暗号化 | SSHトンネルで全通信を暗号化 |
| ポート公開 | VPS側5580はlocalhost bindのみ、外部非公開 |
| 認証 | SSH鍵認証 |
| ZMQ | CurveZMQ不要（SSHトンネル内のため） |

### SSHトンネル

```bash
ssh -L 5580:localhost:5580 user@vps-ip -N -f
```

## 運用フロー

1. VPSで `mt5_relay.py` を起動（MT5ターミナルが起動済みであること）
2. Macで SSHトンネルを張る
3. Claude Code が MCPサーバーを自動起動
4. Claude がツールを呼び出してデータ取得

## VPS側 `.env` 設定

```ini
MT5_ZMQ_PORT=5580
MT5_LOGIN=<口座番号>
MT5_PASSWORD=<パスワード>
MT5_SERVER=<サーバー名>
MT5_TERMINAL_PATH=<terminal64.exeのフルパス>
# 例: C:\Users\MQ\ICSlavePhenix\terminal64.exe
```

> **注意**: `MT5_PORTABLE=true` は設定しないこと。
> ターミナルがポータブルモードで既に起動している場合でも、Python API 側で `portable=True` を渡すと
> 新規インスタンスを起動しようとして IPC 接続に失敗する（エラー -10003）。

`MT5_TERMINAL_PATH` は省略可能だが、省略するとデフォルトパスを探してターミナルが見つからない場合がある。
実際に動いているプロセスのパスを確認して明示的に設定すること：

```powershell
Get-Process terminal64 | Select-Object -ExpandProperty Path
```

## 対象シンボル

初期対応: XAUUSD。将来的に EA_Copier 対応の全10ペアに拡張可能。

## 依存パッケージ

**Mac側 (MT5_mcp/requirements.txt)**:
```
mcp
pyzmq
python-dotenv
```

**VPS側**:
```
MetaTrader5
pyzmq
```
