"""
MT5 MCP Server
==============
VPS上のMT5リレーサーバーにZeroMQ経由で接続し、
MT5のデータをMCPツールとしてClaude Codeに公開する。

マルチ口座設定:
  .envに MT5_ACCOUNTS=名前:ホスト:ポート,名前:ホスト:ポート の形式で指定。
  省略時は MT5_ZMQ_HOST / MT5_ZMQ_PORT で単一口座 "default" として動作。
"""

from __future__ import annotations

import csv
import json
import os
import asyncio
import atexit
from datetime import date as _date, timedelta as _timedelta
from pathlib import Path as _Path

import zmq
import zmq.asyncio
from dotenv import load_dotenv
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

ZMQ_HOST = os.getenv("MT5_ZMQ_HOST", "localhost")
ZMQ_PORT = int(os.getenv("MT5_ZMQ_PORT", "5580"))
ZMQ_TIMEOUT = int(os.getenv("MT5_ZMQ_TIMEOUT", "30000"))


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
            self._reset_socket()
            return {"status": "error", "message": "timeout: VPS relay not responding"}
        except zmq.ZMQError as e:
            self._reset_socket()
            return {"status": "error", "message": f"ZMQ error: {e}"}

    def _reset_socket(self):
        if self._socket and not self._socket.closed:
            self._socket.close()
        self._socket = None


# ---------------------------------------------------------------------------
# マルチ口座クライアント管理
# ---------------------------------------------------------------------------

def _build_account_clients() -> dict[str, ZmqClient]:
    """
    MT5_ACCOUNTS=ICMarkets:host:5580,TitanFX:host:5581 をパース。
    未設定時は MT5_ZMQ_HOST/PORT を "default" として使用。
    """
    accounts_str = os.getenv("MT5_ACCOUNTS", "").strip()
    if accounts_str:
        clients: dict[str, ZmqClient] = {}
        for entry in accounts_str.split(","):
            parts = entry.strip().split(":")
            if len(parts) == 3:
                name, host, port_str = parts
                clients[name.strip()] = ZmqClient(host.strip(), int(port_str.strip()), ZMQ_TIMEOUT)
        if clients:
            return clients
    # フォールバック: 既存の単一口座設定
    return {"default": ZmqClient(ZMQ_HOST, ZMQ_PORT, ZMQ_TIMEOUT)}


account_clients = _build_account_clients()
_default_account = next(iter(account_clients))

for _c in account_clients.values():
    atexit.register(_c._ctx.term)


# ---------------------------------------------------------------------------
# WD_Black キャッシュパス解決
# ---------------------------------------------------------------------------
_WD_BLACK_MT5 = _Path("/Volumes/WD_Black/MT5_cache")
_LOCAL_MT5 = _Path(__file__).resolve().parent / "MT5_cache"

_TICK_FIELDS = ["time_msc", "bid", "ask", "last", "volume", "flags"]


def resolve_mt5_cache_dir() -> _Path:
    """WD_Black があればそこを、なければローカルを使う"""
    if _WD_BLACK_MT5.parent.exists():
        _WD_BLACK_MT5.mkdir(parents=True, exist_ok=True)
        return _WD_BLACK_MT5
    return _LOCAL_MT5


async def _download_ticks_bulk_impl(
    zmq_request,
    symbol: str,
    start: str,
    end: str,
    broker: str,
    cache_root: _Path,
) -> dict:
    """テスト可能な実装本体。zmq_request は関数オブジェクトとして注入する。"""
    start_date = _date.fromisoformat(start)
    end_date = _date.fromisoformat(end)
    total_ticks = 0
    days = 0
    current = start_date

    while current <= end_date:
        day_ticks = []
        for hour in range(24):
            result = await zmq_request("get_ticks_hour", {
                "symbol": symbol,
                "date": current.isoformat(),
                "hour": hour,
            })
            if result.get("status") == "ok":
                day_ticks.extend(result["data"])
            await asyncio.sleep(0.05)

        if day_ticks:
            dir_path = cache_root / broker / symbol.upper() / "ticks"
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / f"{current.isoformat()}.csv"
            with open(file_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=_TICK_FIELDS, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(day_ticks)
            total_ticks += len(day_ticks)
            days += 1

        current += _timedelta(days=1)

    ticks_dir = cache_root / broker / symbol.upper() / "ticks"
    size_mb = (
        sum(f.stat().st_size for f in ticks_dir.glob("*.csv")) / (1024 ** 2)
        if ticks_dir.exists() else 0.0
    )
    return {
        "path": str(ticks_dir),
        "days": days,
        "total_ticks": total_ticks,
        "size_mb": round(size_mb, 2),
    }


def _json_text(data) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps(data, indent=2, ensure_ascii=False, default=str))]


def _error_text(msg: str) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps({"error": msg}, ensure_ascii=False))]


# ---------------------------------------------------------------------------
# ツール共通パラメータ
# ---------------------------------------------------------------------------

_ACCOUNT_PARAM = {
    "account": {
        "type": "string",
        "description": f"口座名。省略時はデフォルト口座 ({_default_account})。list_accounts で一覧取得可能。",
    }
}


app = Server("mt5")

TOOLS = [
    Tool(
        name="list_accounts",
        description="設定済みのMT5口座一覧とデフォルト口座を取得",
        inputSchema={
            "type": "object",
            "properties": {},
            "required": [],
        },
    ),
    Tool(
        name="get_account_info",
        description="MT5口座情報を取得（残高・証拠金・損益・レバレッジ等）",
        inputSchema={
            "type": "object",
            "properties": {**_ACCOUNT_PARAM},
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
                **_ACCOUNT_PARAM,
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
                **_ACCOUNT_PARAM,
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
                **_ACCOUNT_PARAM,
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
                **_ACCOUNT_PARAM,
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
                **_ACCOUNT_PARAM,
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
                **_ACCOUNT_PARAM,
            },
            "required": ["start_time", "end_time"],
        },
    ),
    Tool(
        name="download_ticks_bulk",
        description="指定期間のMT5ティックをCSVでWD_Blackに一括保存（1日×24時間ループ）。WiFi環境で実行すること。",
        inputSchema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "通貨ペア (例: XAUUSD)"},
                "start":  {"type": "string", "description": "開始日 (YYYY-MM-DD)"},
                "end":    {"type": "string", "description": "終了日 (YYYY-MM-DD)"},
                "broker": {"type": "string", "description": "キャッシュ保存先のブローカーフォルダ名。省略時は口座名を使用"},
                **_ACCOUNT_PARAM,
            },
            "required": ["symbol", "start", "end"],
        },
    ),
]

_RELAY_TOOL_NAMES = {t.name for t in TOOLS} - {"list_accounts"}


@app.list_tools()
async def list_tools() -> list[Tool]:
    return TOOLS


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    try:
        if name == "list_accounts":
            return _json_text({
                "accounts": list(account_clients.keys()),
                "default": _default_account,
            })

        # 口座の選択・解決
        account_name = arguments.pop("account", None) or _default_account
        client = account_clients.get(account_name)
        if client is None:
            available = list(account_clients.keys())
            return _error_text(f"不明な口座: '{account_name}'。利用可能: {available}")

        if name == "download_ticks_bulk":
            broker = arguments.pop("broker", None) or account_name
            result = await _download_ticks_bulk_impl(
                zmq_request=client.request,
                symbol=arguments["symbol"],
                start=arguments["start"],
                end=arguments["end"],
                broker=broker,
                cache_root=resolve_mt5_cache_dir(),
            )
            return _json_text(result)

        if name not in _RELAY_TOOL_NAMES:
            return _error_text(f"不明なツール: {name}")

        result = await client.request(name, arguments)

        if result.get("status") == "error":
            return _error_text(result.get("message", "unknown error"))

        return _json_text(result.get("data", {}))

    except Exception as e:
        return _error_text(f"予期しないエラー: {e}")


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
