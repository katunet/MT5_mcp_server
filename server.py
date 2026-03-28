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


import atexit

zmq_client = ZmqClient(ZMQ_HOST, ZMQ_PORT, ZMQ_TIMEOUT)
atexit.register(zmq_client._ctx.term)


def _json_text(data) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps(data, indent=2, ensure_ascii=False, default=str))]


def _error_text(msg: str) -> list[TextContent]:
    return [TextContent(type="text", text=json.dumps({"error": msg}, ensure_ascii=False))]


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

        return _json_text(result.get("data", {}))

    except Exception as e:
        return _error_text(f"予期しないエラー: {e}")


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
