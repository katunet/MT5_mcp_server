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
