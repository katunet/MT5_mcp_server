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
# numpy structured array → list[dict] conversion
# ---------------------------------------------------------------------------
def records_to_dicts(records) -> list[dict]:
    if records is None or len(records) == 0:
        return []
    result = []
    for row in records:
        d = {}
        for name in records.dtype.names:
            val = row[name]
            if hasattr(val, "item"):
                val = val.item()
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
    if rates is None:
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
        # start_time未指定時は最新からcount件を取得
        ticks = mt5.copy_ticks_from(
            symbol, datetime.now(tz=timezone.utc), count, mt5.COPY_TICKS_ALL,
        )
    if ticks is None:
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
