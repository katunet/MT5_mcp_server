# MT5 MCPサーバー実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** VPS上のMT5からZeroMQ経由でデータを取得し、Claude CodeにMCPツールとして公開するサーバーを構築する

**Architecture:** VPS側にPythonリレーサーバー(mt5_relay.py)がMT5 Python APIとZMQ REPソケットを橋渡し。Mac側にMCPサーバー(server.py)がZMQ REQでリクエストを送り、結果をClaudeに返す。通信はSSHトンネル経由。

**Tech Stack:** Python 3, mcp, pyzmq, MetaTrader5 (VPS側), python-dotenv

---

## ファイル構成

```
MT5_mcp/
├─ server.py           # Mac側MCPサーバー（ツール定義 + ZMQクライアント）
├─ mt5_relay.py        # VPS側リレーサーバー（MT5 API + ZMQ REP）
├─ requirements.txt    # Mac側依存 (mcp, pyzmq, python-dotenv)
├─ requirements-vps.txt # VPS側依存 (MetaTrader5, pyzmq)
├─ .env.example        # 接続設定テンプレート
├─ .env                # 実際の接続設定 (gitignore)
├─ .gitignore
└─ tests/
   ├─ test_relay.py    # リレーサーバーのユニットテスト
   └─ test_server.py   # MCPサーバーのユニットテスト
```

---

### Task 1: プロジェクト初期化

**Files:**
- Create: `MT5_mcp/.gitignore`
- Create: `MT5_mcp/requirements.txt`
- Create: `MT5_mcp/requirements-vps.txt`
- Create: `MT5_mcp/.env.example`

- [ ] **Step 1: .gitignore を作成**

```
__pycache__/
*.pyc
.env
*.egg-info/
.venv/
```

- [ ] **Step 2: requirements.txt を作成 (Mac側)**

```
mcp
pyzmq
python-dotenv
```

- [ ] **Step 3: requirements-vps.txt を作成 (VPS側)**

```
MetaTrader5
pyzmq
```

- [ ] **Step 4: .env.example を作成**

```
MT5_ZMQ_HOST=localhost
MT5_ZMQ_PORT=5580
MT5_ZMQ_TIMEOUT=30000
```

- [ ] **Step 5: Mac側の依存をインストール**

Run: `cd /Users/katunet00/EA開発等/MT5_mcp && pip install -r requirements.txt`

- [ ] **Step 6: git init してコミット**

```bash
cd /Users/katunet00/EA開発等/MT5_mcp
git init
git add .gitignore requirements.txt requirements-vps.txt .env.example
git commit -m "chore: initialize MT5 MCP server project"
```

---

### Task 2: VPS側リレーサーバー (`mt5_relay.py`)

**Files:**
- Create: `MT5_mcp/mt5_relay.py`

- [ ] **Step 1: リレーサーバーのテストを作成**

Create `MT5_mcp/tests/test_relay.py`:

```python
"""mt5_relay のコマンドディスパッチとレスポンス形式のテスト。

MT5 Python API はモックし、ZMQ通信もモックする。
リレーサーバーのコマンド解析・ハンドラー呼び出し・JSONレスポンス生成をテスト。
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import numpy as np

# mt5_relay から関数をインポート (Step 3 で実装)
from mt5_relay import dispatch_command, make_response, make_error


class TestMakeResponse:
    def test_ok_response(self):
        resp = make_response({"balance": 10000})
        parsed = json.loads(resp)
        assert parsed["status"] == "ok"
        assert parsed["data"]["balance"] == 10000

    def test_error_response(self):
        resp = make_error("MT5 not connected")
        parsed = json.loads(resp)
        assert parsed["status"] == "error"
        assert "MT5 not connected" in parsed["message"]

    def test_error_with_code(self):
        resp = make_error("Request failed", code=10004)
        parsed = json.loads(resp)
        assert parsed["code"] == 10004


class TestDispatchCommand:
    @patch("mt5_relay.mt5")
    def test_get_account_info(self, mock_mt5):
        mock_info = MagicMock()
        mock_info._asdict.return_value = {
            "login": 12345,
            "balance": 10000.0,
            "equity": 10500.0,
            "margin": 200.0,
            "margin_free": 10300.0,
            "profit": 500.0,
            "currency": "USD",
            "server": "TitanFX-01",
            "leverage": 500,
        }
        mock_mt5.account_info.return_value = mock_info

        result = json.loads(dispatch_command("get_account_info", {}))
        assert result["status"] == "ok"
        assert result["data"]["balance"] == 10000.0
        assert result["data"]["equity"] == 10500.0

    @patch("mt5_relay.mt5")
    def test_get_account_info_failure(self, mock_mt5):
        mock_mt5.account_info.return_value = None
        mock_mt5.last_error.return_value = (10004, "Request failed")

        result = json.loads(dispatch_command("get_account_info", {}))
        assert result["status"] == "error"

    @patch("mt5_relay.mt5")
    def test_get_positions(self, mock_mt5):
        mock_pos = MagicMock()
        mock_pos._asdict.return_value = {
            "ticket": 100001,
            "symbol": "XAUUSD",
            "type": 0,
            "volume": 0.1,
            "price_open": 2300.0,
            "sl": 2290.0,
            "tp": 2320.0,
            "profit": 50.0,
        }
        mock_mt5.positions_get.return_value = (mock_pos,)

        result = json.loads(dispatch_command("get_positions", {}))
        assert result["status"] == "ok"
        assert len(result["data"]) == 1
        assert result["data"][0]["symbol"] == "XAUUSD"

    @patch("mt5_relay.mt5")
    def test_get_positions_with_symbol(self, mock_mt5):
        mock_mt5.positions_get.return_value = ()

        result = json.loads(dispatch_command("get_positions", {"symbol": "XAUUSD"}))
        assert result["status"] == "ok"
        assert result["data"] == []
        mock_mt5.positions_get.assert_called_once_with(symbol="XAUUSD")

    @patch("mt5_relay.mt5")
    def test_get_symbol_info(self, mock_mt5):
        mock_info = MagicMock()
        mock_info._asdict.return_value = {
            "name": "XAUUSD",
            "point": 0.01,
            "digits": 2,
            "spread": 20,
            "trade_contract_size": 100.0,
            "volume_min": 0.01,
            "volume_max": 100.0,
            "volume_step": 0.01,
            "swap_long": -5.0,
            "swap_short": 1.0,
        }
        mock_mt5.symbol_info.return_value = mock_info

        result = json.loads(dispatch_command("get_symbol_info", {"symbol": "XAUUSD"}))
        assert result["status"] == "ok"
        assert result["data"]["point"] == 0.01

    @patch("mt5_relay.mt5")
    def test_get_ohlcv(self, mock_mt5):
        # MT5 copy_rates_from_pos returns numpy structured array
        data = np.array(
            [(1711612800, 2300.0, 2310.0, 2295.0, 2305.0, 100, 20, 0)],
            dtype=[
                ("time", "i8"), ("open", "f8"), ("high", "f8"),
                ("low", "f8"), ("close", "f8"), ("tick_volume", "i8"),
                ("spread", "i4"), ("real_volume", "i8"),
            ],
        )
        mock_mt5.copy_rates_from_pos.return_value = data
        mock_mt5.TIMEFRAME_H1 = 16385

        result = json.loads(dispatch_command("get_ohlcv", {
            "symbol": "XAUUSD", "timeframe": "H1", "count": 100,
        }))
        assert result["status"] == "ok"
        assert len(result["data"]) == 1
        assert result["data"][0]["open"] == 2300.0

    @patch("mt5_relay.mt5")
    def test_get_ticks(self, mock_mt5):
        data = np.array(
            [(1711612800, 2300.5, 2301.0, 0.0, 0, 0, 0, 0)],
            dtype=[
                ("time", "i8"), ("bid", "f8"), ("ask", "f8"),
                ("last", "f8"), ("volume", "i8"), ("time_msc", "i8"),
                ("flags", "i4"), ("volume_real", "f8"),
            ],
        )
        mock_mt5.copy_ticks_from.return_value = data
        mock_mt5.COPY_TICKS_ALL = 1

        result = json.loads(dispatch_command("get_ticks", {
            "symbol": "XAUUSD", "count": 100,
        }))
        assert result["status"] == "ok"
        assert len(result["data"]) == 1
        assert result["data"][0]["bid"] == 2300.5

    def test_unknown_command(self):
        result = json.loads(dispatch_command("nonexistent", {}))
        assert result["status"] == "error"
        assert "unknown command" in result["message"]
```

- [ ] **Step 2: テストを実行して失敗を確認**

Run: `cd /Users/katunet00/EA開発等/MT5_mcp && python -m pytest tests/test_relay.py -v`
Expected: FAIL (ImportError: cannot import from mt5_relay)

- [ ] **Step 3: mt5_relay.py を実装**

Create `MT5_mcp/mt5_relay.py`:

```python
"""
MT5 Relay Server
================
VPS上で動作し、MetaTrader5 Python APIのデータをZeroMQ REP経由で公開する。
Mac側のMCPサーバーからのリクエストを受けてMT5にクエリし、JSONで返す。
"""

import json
import sys
import time
import logging
from datetime import datetime, timezone

import zmq

try:
    import MetaTrader5 as mt5
except ImportError:
    # テスト時やMac上ではMetaTrader5パッケージが無い
    mt5 = None

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("mt5_relay.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("MT5Relay")

# ---------------------------------------------------------------------------
# Timeframe mapping
# ---------------------------------------------------------------------------
TIMEFRAME_MAP = {
    "M1": "TIMEFRAME_M1",
    "M5": "TIMEFRAME_M5",
    "M15": "TIMEFRAME_M15",
    "M30": "TIMEFRAME_M30",
    "H1": "TIMEFRAME_H1",
    "H4": "TIMEFRAME_H4",
    "D1": "TIMEFRAME_D1",
    "W1": "TIMEFRAME_W1",
    "MN1": "TIMEFRAME_MN1",
}


def get_mt5_timeframe(tf_str: str):
    """文字列のtimeframeをMT5定数に変換する。"""
    attr_name = TIMEFRAME_MAP.get(tf_str.upper())
    if attr_name is None:
        raise ValueError(f"Unknown timeframe: {tf_str}")
    return getattr(mt5, attr_name)


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------
def make_response(data) -> str:
    return json.dumps({"status": "ok", "data": data}, ensure_ascii=False, default=str)


def make_error(message: str, code: int | None = None) -> str:
    resp = {"status": "error", "message": message}
    if code is not None:
        resp["code"] = code
    return json.dumps(resp, ensure_ascii=False)


# ---------------------------------------------------------------------------
# numpy structured array → list[dict] 変換
# ---------------------------------------------------------------------------
def records_to_dicts(records) -> list[dict]:
    """numpy structured arrayをlist[dict]に変換。timeフィールドはISO文字列にする。"""
    if records is None or len(records) == 0:
        return []
    result = []
    for row in records:
        d = {}
        for name in records.dtype.names:
            val = row[name]
            # numpy型をPythonネイティブに変換
            if hasattr(val, "item"):
                val = val.item()
            # timeフィールドはUNIXタイムスタンプ→ISO文字列
            if name == "time":
                val = datetime.fromtimestamp(val, tz=timezone.utc).isoformat()
            elif name == "time_msc":
                val = datetime.fromtimestamp(val / 1000, tz=timezone.utc).isoformat()
            d[name] = val
        result.append(d)
    return result


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------
def handle_get_account_info(params: dict) -> str:
    info = mt5.account_info()
    if info is None:
        code, msg = mt5.last_error()
        return make_error(f"account_info failed: {msg}", code)
    return make_response(info._asdict())


def handle_get_positions(params: dict) -> str:
    symbol = params.get("symbol")
    if symbol:
        positions = mt5.positions_get(symbol=symbol)
    else:
        positions = mt5.positions_get()
    if positions is None:
        code, msg = mt5.last_error()
        return make_error(f"positions_get failed: {msg}", code)
    return make_response([p._asdict() for p in positions])


def handle_get_ohlcv(params: dict) -> str:
    symbol = params.get("symbol", "XAUUSD")
    tf_str = params.get("timeframe", "H1")
    count = params.get("count", 1000)

    try:
        tf = get_mt5_timeframe(tf_str)
    except ValueError as e:
        return make_error(str(e))

    start_time = params.get("start_time")
    if start_time:
        dt = datetime.fromisoformat(start_time).replace(tzinfo=timezone.utc)
        rates = mt5.copy_rates_from(symbol, tf, dt, count)
    else:
        rates = mt5.copy_rates_from_pos(symbol, tf, 0, count)

    if rates is None or len(rates) == 0:
        code, msg = mt5.last_error()
        return make_error(f"copy_rates failed: {msg}", code)
    return make_response(records_to_dicts(rates))


def handle_get_ticks(params: dict) -> str:
    symbol = params.get("symbol", "XAUUSD")
    count = params.get("count", 1000)

    start_time = params.get("start_time")
    if start_time:
        dt = datetime.fromisoformat(start_time).replace(tzinfo=timezone.utc)
        ticks = mt5.copy_ticks_from(symbol, dt, count, mt5.COPY_TICKS_ALL)
    else:
        ticks = mt5.copy_ticks_from(
            symbol,
            datetime(2000, 1, 1, tzinfo=timezone.utc),
            count,
            mt5.COPY_TICKS_ALL,
        )

    if ticks is None or len(ticks) == 0:
        code, msg = mt5.last_error()
        return make_error(f"copy_ticks failed: {msg}", code)
    return make_response(records_to_dicts(ticks))


def handle_get_symbol_info(params: dict) -> str:
    symbol = params.get("symbol", "XAUUSD")
    info = mt5.symbol_info(symbol)
    if info is None:
        code, msg = mt5.last_error()
        return make_error(f"symbol_info failed: {msg}", code)
    return make_response(info._asdict())


def handle_get_history_deals(params: dict) -> str:
    start = params.get("start_time")
    end = params.get("end_time")
    if not start or not end:
        return make_error("start_time and end_time are required")

    dt_from = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    dt_to = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)

    symbol = params.get("symbol")
    if symbol:
        deals = mt5.history_deals_get(dt_from, dt_to, group=f"*{symbol}*")
    else:
        deals = mt5.history_deals_get(dt_from, dt_to)

    if deals is None:
        code, msg = mt5.last_error()
        return make_error(f"history_deals_get failed: {msg}", code)
    return make_response([d._asdict() for d in deals])


def handle_get_history_orders(params: dict) -> str:
    start = params.get("start_time")
    end = params.get("end_time")
    if not start or not end:
        return make_error("start_time and end_time are required")

    dt_from = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    dt_to = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)

    symbol = params.get("symbol")
    if symbol:
        orders = mt5.history_orders_get(dt_from, dt_to, group=f"*{symbol}*")
    else:
        orders = mt5.history_orders_get(dt_from, dt_to)

    if orders is None:
        code, msg = mt5.last_error()
        return make_error(f"history_orders_get failed: {msg}", code)
    return make_response([o._asdict() for o in orders])


# ---------------------------------------------------------------------------
# Command dispatcher
# ---------------------------------------------------------------------------
COMMANDS = {
    "get_account_info": handle_get_account_info,
    "get_positions": handle_get_positions,
    "get_ohlcv": handle_get_ohlcv,
    "get_ticks": handle_get_ticks,
    "get_symbol_info": handle_get_symbol_info,
    "get_history_deals": handle_get_history_deals,
    "get_history_orders": handle_get_history_orders,
}


def dispatch_command(command: str, params: dict) -> str:
    handler = COMMANDS.get(command)
    if handler is None:
        return make_error(f"unknown command: {command}")
    try:
        return handler(params)
    except Exception as e:
        logger.exception(f"Handler error for {command}")
        return make_error(f"internal error: {e}")


# ---------------------------------------------------------------------------
# MT5 connection management
# ---------------------------------------------------------------------------
def ensure_mt5_connected() -> bool:
    """MT5接続を確認し、切れていたら再接続する。"""
    if mt5 is None:
        return False
    try:
        info = mt5.account_info()
        if info is not None:
            return True
    except Exception:
        pass

    logger.info("MT5 reconnecting...")
    mt5.shutdown()
    if not mt5.initialize():
        logger.error(f"MT5 initialize failed: {mt5.last_error()}")
        return False
    logger.info("MT5 reconnected successfully")
    return True


# ---------------------------------------------------------------------------
# Main server loop
# ---------------------------------------------------------------------------
def main():
    host = "127.0.0.1"
    port = 5580

    if mt5 is None:
        logger.error("MetaTrader5 package not available. Install on Windows VPS.")
        sys.exit(1)

    if not mt5.initialize():
        logger.error(f"MT5 initialize failed: {mt5.last_error()}")
        sys.exit(1)

    info = mt5.account_info()
    logger.info(f"MT5 connected: account={info.login}, server={info.server}")

    ctx = zmq.Context()
    socket = ctx.socket(zmq.REP)
    socket.bind(f"tcp://{host}:{port}")
    logger.info(f"ZMQ REP listening on tcp://{host}:{port}")

    try:
        while True:
            raw = socket.recv_string()
            logger.debug(f"[RECV] {raw}")

            try:
                request = json.loads(raw)
            except (json.JSONDecodeError, TypeError) as e:
                response = make_error(f"invalid JSON: {e}")
                socket.send_string(response)
                continue

            command = request.get("command", "")
            params = request.get("params", {})

            if not ensure_mt5_connected():
                response = make_error("MT5 not connected")
            else:
                response = dispatch_command(command, params)

            logger.debug(f"[SEND] {response[:200]}...")
            socket.send_string(response)

    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        socket.close()
        ctx.term()
        mt5.shutdown()
        logger.info("MT5 Relay stopped")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: テストを実行して全パスを確認**

Run: `cd /Users/katunet00/EA開発等/MT5_mcp && python -m pytest tests/test_relay.py -v`
Expected: All tests PASS

- [ ] **Step 5: コミット**

```bash
cd /Users/katunet00/EA開発等/MT5_mcp
git add mt5_relay.py tests/test_relay.py
git commit -m "feat: add VPS relay server with MT5 Python API + ZMQ REP"
```

---

### Task 3: Mac側MCPサーバー (`server.py`)

**Files:**
- Create: `MT5_mcp/server.py`

- [ ] **Step 1: MCPサーバーのテストを作成**

Create `MT5_mcp/tests/test_server.py`:

```python
"""server.py のMCPツール定義とZMQクライアントのテスト。

ZMQ通信はモックし、ツール呼び出し→リクエスト送信→レスポンスパースをテスト。
"""

import json
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from server import TOOLS, call_tool, ZmqClient


class TestToolDefinitions:
    def test_all_tools_defined(self):
        names = {t.name for t in TOOLS}
        expected = {
            "get_account_info", "get_positions", "get_ohlcv",
            "get_ticks", "get_symbol_info",
            "get_history_deals", "get_history_orders",
        }
        assert names == expected

    def test_get_ohlcv_schema(self):
        tool = next(t for t in TOOLS if t.name == "get_ohlcv")
        props = tool.inputSchema["properties"]
        assert "symbol" in props
        assert "timeframe" in props
        assert "count" in props
        assert tool.inputSchema["required"] == ["symbol", "timeframe", "count"]

    def test_get_ticks_schema(self):
        tool = next(t for t in TOOLS if t.name == "get_ticks")
        props = tool.inputSchema["properties"]
        assert "symbol" in props
        assert "count" in props
        assert tool.inputSchema["required"] == ["symbol", "count"]


class TestZmqClient:
    def test_build_request(self):
        client = ZmqClient.__new__(ZmqClient)
        req = client._build_request("get_ohlcv", {"symbol": "XAUUSD", "timeframe": "H1", "count": 100})
        assert req == json.dumps({
            "command": "get_ohlcv",
            "params": {"symbol": "XAUUSD", "timeframe": "H1", "count": 100},
        })


class TestCallTool:
    @pytest.mark.asyncio
    @patch("server.zmq_client")
    async def test_get_account_info(self, mock_client):
        mock_client.request = AsyncMock(return_value={
            "status": "ok",
            "data": {"balance": 10000.0, "equity": 10500.0},
        })
        result = await call_tool("get_account_info", {})
        assert len(result) == 1
        text = json.loads(result[0].text)
        assert text["balance"] == 10000.0

    @pytest.mark.asyncio
    @patch("server.zmq_client")
    async def test_error_response(self, mock_client):
        mock_client.request = AsyncMock(return_value={
            "status": "error",
            "message": "MT5 not connected",
        })
        result = await call_tool("get_account_info", {})
        assert len(result) == 1
        text = json.loads(result[0].text)
        assert "error" in text

    @pytest.mark.asyncio
    @patch("server.zmq_client")
    async def test_get_ohlcv(self, mock_client):
        mock_client.request = AsyncMock(return_value={
            "status": "ok",
            "data": [{"time": "2026-03-28T00:00:00+00:00", "open": 2300.0}],
        })
        result = await call_tool("get_ohlcv", {
            "symbol": "XAUUSD", "timeframe": "H1", "count": 100,
        })
        text = json.loads(result[0].text)
        assert isinstance(text, list)
        assert text[0]["open"] == 2300.0

    @pytest.mark.asyncio
    @patch("server.zmq_client")
    async def test_unknown_tool(self, mock_client):
        result = await call_tool("nonexistent", {})
        text = json.loads(result[0].text)
        assert "error" in text
```

- [ ] **Step 2: テストを実行して失敗を確認**

Run: `cd /Users/katunet00/EA開発等/MT5_mcp && python -m pytest tests/test_server.py -v`
Expected: FAIL (ImportError)

- [ ] **Step 3: server.py を実装**

Create `MT5_mcp/server.py`:

```python
"""
MT5 MCP Server
==============
VPS上のMT5リレーサーバーにZeroMQ経由で接続し、
MT5のデータをMCPツールとしてClaude Codeに公開する。
"""

from __future__ import annotations

import json
import os
import asyncio

import zmq
import zmq.asyncio
from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

# .env を読み込み
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

ZMQ_HOST = os.getenv("MT5_ZMQ_HOST", "localhost")
ZMQ_PORT = int(os.getenv("MT5_ZMQ_PORT", "5580"))
ZMQ_TIMEOUT = int(os.getenv("MT5_ZMQ_TIMEOUT", "30000"))


# ---------------------------------------------------------------------------
# ZMQ Client
# ---------------------------------------------------------------------------
class ZmqClient:
    def __init__(self, host: str, port: int, timeout: int):
        self.endpoint = f"tcp://{host}:{port}"
        self.timeout = timeout
        self._ctx = zmq.asyncio.Context()
        self._socket: zmq.asyncio.Socket | None = None

    def _ensure_socket(self) -> zmq.asyncio.Socket:
        if self._socket is None or self._socket.closed:
            self._socket = self._ctx.socket(zmq.REQ)
            self._socket.setsockopt(zmq.RCVTIMEO, self.timeout)
            self._socket.setsockopt(zmq.SNDTIMEO, self.timeout)
            self._socket.setsockopt(zmq.LINGER, 0)
            self._socket.connect(self.endpoint)
        return self._socket

    def _build_request(self, command: str, params: dict) -> str:
        return json.dumps({"command": command, "params": params})

    async def request(self, command: str, params: dict) -> dict:
        sock = self._ensure_socket()
        msg = self._build_request(command, params)
        try:
            await sock.send_string(msg)
            raw = await sock.recv_string()
            return json.loads(raw)
        except zmq.Again:
            # タイムアウト: ソケットを再作成 (REQソケットは状態が壊れる)
            self._reset_socket()
            return {"status": "error", "message": "timeout: VPS relay not responding"}
        except zmq.ZMQError as e:
            self._reset_socket()
            return {"status": "error", "message": f"ZMQ error: {e}"}

    def _reset_socket(self):
        if self._socket and not self._socket.closed:
            self._socket.close()
        self._socket = None


zmq_client = ZmqClient(ZMQ_HOST, ZMQ_PORT, ZMQ_TIMEOUT)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _json_text(data) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps(data, indent=2, ensure_ascii=False, default=str))]


def _error_text(msg: str) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps({"error": msg}, ensure_ascii=False))]


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------
app = Server("mt5")

TOOLS = [
    Tool(
        name="get_account_info",
        description="MT5口座情報を取得（残高・証拠金・損益・レバレッジ等）",
        inputSchema={
            "type": "object",
            "properties": {},
            "required": [],
        },
    ),
    Tool(
        name="get_positions",
        description="MT5のオープンポジション一覧を取得",
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "通貨ペア（例: XAUUSD）。省略時は全ポジション",
                },
            },
            "required": [],
        },
    ),
    Tool(
        name="get_ohlcv",
        description="OHLCVローソク足データを取得。バックテスト用の価格データ取得に使用。",
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "通貨ペア（例: XAUUSD）",
                },
                "timeframe": {
                    "type": "string",
                    "description": "時間足（M1, M5, M15, M30, H1, H4, D1, W1, MN1）",
                },
                "count": {
                    "type": "integer",
                    "description": "取得本数（最大100000）",
                },
                "start_time": {
                    "type": "string",
                    "description": "開始日時 ISO 8601（例: 2026-01-01T00:00:00）。省略時は最新からcount本",
                },
            },
            "required": ["symbol", "timeframe", "count"],
        },
    ),
    Tool(
        name="get_ticks",
        description="ティックデータ（Bid/Ask履歴）を取得。スプレッド変動の再現やティックレベルのバックテストに使用。",
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "通貨ペア（例: XAUUSD）",
                },
                "count": {
                    "type": "integer",
                    "description": "取得件数（最大1000000）",
                },
                "start_time": {
                    "type": "string",
                    "description": "開始日時 ISO 8601。省略時は直近のcount件",
                },
            },
            "required": ["symbol", "count"],
        },
    ),
    Tool(
        name="get_symbol_info",
        description="シンボルの取引条件を取得（スプレッド・スワップ・ロットサイズ・ポイント等）。バックテストのパラメータ設定に使用。",
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "通貨ペア（例: XAUUSD）",
                },
            },
            "required": ["symbol"],
        },
    ),
    Tool(
        name="get_history_deals",
        description="約定履歴を取得（期間指定）",
        inputSchema={
            "type": "object",
            "properties": {
                "start_time": {
                    "type": "string",
                    "description": "開始日時 ISO 8601（例: 2026-01-01T00:00:00）",
                },
                "end_time": {
                    "type": "string",
                    "description": "終了日時 ISO 8601",
                },
                "symbol": {
                    "type": "string",
                    "description": "通貨ペアでフィルタ（省略時は全シンボル）",
                },
            },
            "required": ["start_time", "end_time"],
        },
    ),
    Tool(
        name="get_history_orders",
        description="注文履歴を取得（期間指定）",
        inputSchema={
            "type": "object",
            "properties": {
                "start_time": {
                    "type": "string",
                    "description": "開始日時 ISO 8601",
                },
                "end_time": {
                    "type": "string",
                    "description": "終了日時 ISO 8601",
                },
                "symbol": {
                    "type": "string",
                    "description": "通貨ペアでフィルタ（省略時は全シンボル）",
                },
            },
            "required": ["start_time", "end_time"],
        },
    ),
]


@app.list_tools()
async def list_tools() -> list[Tool]:
    return TOOLS


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        if name not in {t.name for t in TOOLS}:
            return _error_text(f"不明なツール: {name}")

        result = await zmq_client.request(name, arguments)

        if result.get("status") == "error":
            return _error_text(result.get("message", "unknown error"))

        return _json_text(result["data"])

    except Exception as e:
        return _error_text(f"予期しないエラー: {e}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 4: テストを実行して全パスを確認**

Run: `cd /Users/katunet00/EA開発等/MT5_mcp && python -m pytest tests/test_server.py -v`
Expected: All tests PASS

- [ ] **Step 5: コミット**

```bash
cd /Users/katunet00/EA開発等/MT5_mcp
git add server.py tests/test_server.py
git commit -m "feat: add MCP server with ZMQ client for MT5 data access"
```

---

### Task 4: .env と MCP 登録

**Files:**
- Create: `MT5_mcp/.env`
- Modify: Claude Code settings.json

- [ ] **Step 1: .env を作成**

```
MT5_ZMQ_HOST=localhost
MT5_ZMQ_PORT=5580
MT5_ZMQ_TIMEOUT=30000
```

- [ ] **Step 2: Claude Code の settings.json に MCP サーバーを登録**

`~/.claude/settings.json` の `mcpServers` に追加:

```json
{
  "mt5": {
    "command": "python3",
    "args": ["server.py"],
    "cwd": "/Users/katunet00/EA開発等/MT5_mcp"
  }
}
```

- [ ] **Step 3: コミット**

```bash
cd /Users/katunet00/EA開発等/MT5_mcp
git add .env.example
git commit -m "docs: add env example and MCP registration instructions"
```

---

### Task 5: 結合テスト (SSH トンネル + VPS接続)

**Files:** なし（手動テスト手順）

- [ ] **Step 1: VPSにmt5_relay.pyとrequirements-vps.txtを配置**

VPSにファイルをコピー（scpまたはSyncthing経由）:
```bash
scp mt5_relay.py requirements-vps.txt user@vps-ip:~/mt5_relay/
```

- [ ] **Step 2: VPS上で依存インストールとリレーサーバー起動**

VPS上で:
```bash
cd ~/mt5_relay
pip install -r requirements-vps.txt
python mt5_relay.py
```

Expected: `MT5 connected: account=XXXXX, server=...` と `ZMQ REP listening on tcp://127.0.0.1:5580` が表示

- [ ] **Step 3: Mac側でSSHトンネルを張る**

```bash
ssh -L 5580:localhost:5580 user@vps-ip -N -f
```

- [ ] **Step 4: MCP サーバーの動作確認**

Claude Code を起動して以下のツールを呼び出してテスト:

1. `get_account_info` → 口座残高等が返ること
2. `get_symbol_info(symbol="XAUUSD")` → スプレッド・ポイント等が返ること
3. `get_ohlcv(symbol="XAUUSD", timeframe="H1", count=10)` → 10本のローソク足データが返ること
4. `get_ticks(symbol="XAUUSD", count=10)` → 10件のティックデータが返ること

- [ ] **Step 5: 確認完了後コミット**

```bash
cd /Users/katunet00/EA開発等/MT5_mcp
git add -A
git commit -m "chore: integration test verified"
```
