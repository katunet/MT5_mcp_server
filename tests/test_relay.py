"""mt5_relay のコマンドディスパッチとレスポンス形式のテスト。

MT5 Python API はモックし、ZMQ通信もモックする。
リレーサーバーのコマンド解析・ハンドラー呼び出し・JSONレスポンス生成をテスト。
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import numpy as np

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
            "ticket": 100001, "symbol": "XAUUSD", "type": 0,
            "volume": 0.1, "price_open": 2300.0,
            "sl": 2290.0, "tp": 2320.0, "profit": 50.0,
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
            "name": "XAUUSD", "point": 0.01, "digits": 2, "spread": 20,
            "trade_contract_size": 100.0, "volume_min": 0.01,
            "volume_max": 100.0, "volume_step": 0.01,
            "swap_long": -5.0, "swap_short": 1.0,
        }
        mock_mt5.symbol_info.return_value = mock_info
        result = json.loads(dispatch_command("get_symbol_info", {"symbol": "XAUUSD"}))
        assert result["status"] == "ok"
        assert result["data"]["point"] == 0.01

    @patch("mt5_relay.mt5")
    def test_get_ohlcv(self, mock_mt5):
        data = np.array(
            [(1711612800, 2300.0, 2310.0, 2295.0, 2305.0, 100, 20, 0)],
            dtype=[("time", "i8"), ("open", "f8"), ("high", "f8"),
                   ("low", "f8"), ("close", "f8"), ("tick_volume", "i8"),
                   ("spread", "i4"), ("real_volume", "i8")],
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
            dtype=[("time", "i8"), ("bid", "f8"), ("ask", "f8"),
                   ("last", "f8"), ("volume", "i8"), ("time_msc", "i8"),
                   ("flags", "i4"), ("volume_real", "f8")],
        )
        mock_mt5.copy_ticks_from.return_value = data
        mock_mt5.COPY_TICKS_ALL = 1
        result = json.loads(dispatch_command("get_ticks", {
            "symbol": "XAUUSD", "count": 100,
        }))
        assert result["status"] == "ok"
        assert len(result["data"]) == 1
        assert result["data"][0]["bid"] == 2300.5

    @patch("mt5_relay.mt5")
    def test_get_history_deals(self, mock_mt5):
        mock_deal = MagicMock()
        mock_deal._asdict.return_value = {
            "ticket": 200001, "order": 300001, "symbol": "XAUUSD",
            "type": 0, "entry": 0, "volume": 0.1,
            "price": 2300.0, "profit": 50.0, "commission": -1.0,
            "swap": 0.0, "time": 1735689600,
        }
        mock_mt5.history_deals_get.return_value = (mock_deal,)
        result = json.loads(dispatch_command("get_history_deals", {
            "start_time": "2026-01-01T00:00:00",
            "end_time": "2026-03-28T00:00:00",
        }))
        assert result["status"] == "ok"
        assert len(result["data"]) == 1

    @patch("mt5_relay.mt5")
    def test_get_history_orders(self, mock_mt5):
        mock_order = MagicMock()
        mock_order._asdict.return_value = {
            "ticket": 300001, "symbol": "XAUUSD", "type": 0,
            "volume_initial": 0.1, "volume_current": 0.0,
            "price_open": 2300.0, "sl": 2290.0, "tp": 2320.0,
            "price_current": 2305.0, "state": 3, "time_setup": 1735689600,
        }
        mock_mt5.history_orders_get.return_value = (mock_order,)
        result = json.loads(dispatch_command("get_history_orders", {
            "start_time": "2026-01-01T00:00:00",
            "end_time": "2026-03-28T00:00:00",
        }))
        assert result["status"] == "ok"
        assert len(result["data"]) == 1

    def test_unknown_command(self):
        result = json.loads(dispatch_command("nonexistent", {}))
        assert result["status"] == "error"
        assert "unknown command" in result["message"]
