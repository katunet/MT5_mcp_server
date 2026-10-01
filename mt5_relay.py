"""
MT5 Relay Server
================
VPS上で動作し、MetaTrader5 Python APIのデータをZeroMQ REP経由で公開する。
Mac側のMCPサーバーからのリクエストを受けてMT5にクエリし、JSONで返す。

起動例（複数口座）:
  # IC Markets（デフォルト .env）
  python mt5_relay.py

  # TitanFX（別 .env ファイル + 別ポート）
  python mt5_relay.py --port 5581 --env .env.titanfx
"""

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

import zmq
from dotenv import load_dotenv

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


def handle_get_ticks_hour(params: dict) -> str:
    """1時間分のティックを copy_ticks_range で取得する。

    params:
        symbol: str   通貨ペア (例: "XAUUSD")
        date:   str   日付 "YYYY-MM-DD"
        hour:   int   時間 0-23 (UTC)
    """
    symbol = params.get("symbol", "XAUUSD")
    date_str = params.get("date")
    if not date_str:
        return make_error("date is required")
    hour = int(params.get("hour", 0))

    try:
        y, m, d = (int(x) for x in date_str.split("-"))
    except (ValueError, KeyError) as e:
        return make_error(f"invalid date: {e}")

    dt_from = datetime(y, m, d, hour, 0, 0, tzinfo=timezone.utc)
    dt_to = dt_from + timedelta(hours=1)

    ticks = mt5.copy_ticks_range(symbol, dt_from, dt_to, mt5.COPY_TICKS_ALL)
    if ticks is None:
        code, msg = mt5.last_error()
        return make_error(f"copy_ticks_range failed: {msg}", code)
    return make_response(records_to_dicts(ticks))


# ---------------------------------------------------------------------------
# Backtest state（プロセス間共有しないのでスレッドロックで保護）
# ---------------------------------------------------------------------------
_bt_lock = threading.Lock()
_bt_state: dict = {
    "process": None,
    "report_path": None,
    "ini_path": None,
    "status": "idle",   # idle / running / completed / failed
    "exit_code": None,
}


def _resolve_metatester() -> str:
    """metatester64.exe のパスを環境変数から解決する。"""
    path = os.environ.get("MT5_TESTER_PATH", "").strip()
    if path and os.path.exists(path):
        return path
    # MT5_TERMINAL_PATH の隣にある metatester64.exe を推測
    terminal = os.environ.get("MT5_TERMINAL_PATH", "").strip()
    if terminal:
        candidate = os.path.join(os.path.dirname(terminal), "metatester64.exe")
        if os.path.exists(candidate):
            return candidate
    return ""


def _build_ini(params: dict, report_path: str) -> str:
    """バックテスト用 INI ファイル内容を生成する。"""
    tester = {
        "Expert":           params.get("expert", r"Advisors\AUDCADZone_EA"),
        "Symbol":           params.get("symbol", "AUDCAD"),
        "Period":           params.get("period", "H1"),
        "Deposit":          str(params.get("deposit", 10000)),
        "Currency":         params.get("currency", "USD"),
        "Leverage":         str(params.get("leverage", 100)),
        "FromDate":         params.get("from_date", "2024.01.01"),
        "ToDate":           params.get("to_date", "2026.04.01"),
        "Model":            str(params.get("model", 1)),   # 1=Open prices only
        "Optimization":     str(params.get("optimization", 0)),
        "Report":           report_path,
        "ReplaceReport":    "1",
        "ShutdownTerminal": "1",
        "UseLocal":         "1",
    }
    lines = ["[Tester]"] + [f"{k}={v}" for k, v in tester.items()]

    inputs: dict = params.get("inputs", {})
    if inputs:
        lines += ["", "[TesterInputs]"] + [f"{k}={v}" for k, v in inputs.items()]

    return "\n".join(lines)


def _parse_backtest_xml(xml_path: str) -> dict:
    """バックテスト結果 XML から主要指標を抽出する。"""
    tree = ET.parse(xml_path)
    root = tree.getroot()
    result: dict = {}
    # MT5 のレポート XML は <Row Name="..." Value="..."/> 形式
    for row in root.iter("Row"):
        name = row.get("Name", "")
        value = row.get("Value", "")
        if name:
            result[name] = value
    return result


def _parse_backtest_html(html: str) -> dict:
    """バックテスト結果 HTML から主要指標を正規表現で抽出する。"""
    patterns = {
        "total_trades":  r"Total Trades[^<]*?</td>\s*<td[^>]*>([0-9,]+)",
        "profit_factor": r"Profit Factor[^<]*?</td>\s*<td[^>]*>([0-9.]+)",
        "max_drawdown":  r"Maximal drawdown[^<]*?</td>\s*<td[^>]*>([0-9., ]+)",
        "net_profit":    r"Net Profit[^<]*?</td>\s*<td[^>]*>([0-9., -]+)",
        "gross_profit":  r"Gross Profit[^<]*?</td>\s*<td[^>]*>([0-9., ]+)",
        "gross_loss":    r"Gross Loss[^<]*?</td>\s*<td[^>]*>([0-9., -]+)",
    }
    result: dict = {}
    for key, pat in patterns.items():
        m = re.search(pat, html, re.IGNORECASE | re.DOTALL)
        if m:
            result[key] = m.group(1).strip()
    return result


def handle_run_backtest(params: dict) -> str:
    """metatester64.exe をサブプロセスで起動してバックテストを開始する。
    処理は非同期（即座に started を返す）。状態は get_backtest_status で確認する。
    """
    with _bt_lock:
        if _bt_state["status"] == "running":
            return make_error("バックテストが既に実行中です（get_backtest_status で確認してください）")

        tester_path = _resolve_metatester()
        if not tester_path:
            return make_error(
                "metatester64.exe が見つかりません。"
                ".env に MT5_TESTER_PATH=<metatester64.exeの絶対パス> を設定してください。"
            )

        report_path = params.get(
            "report_path",
            r"C:\Users\katun\EA開発\analysis_orchestra\reports\bt_result",
        )
        os.makedirs(os.path.dirname(report_path), exist_ok=True)

        ini_content = _build_ini(params, report_path)
        ini_path = os.path.join(os.path.dirname(tester_path), "bt_mcp_temp.ini")
        try:
            with open(ini_path, "w", encoding="utf-8") as f:
                f.write(ini_content)
        except OSError as e:
            return make_error(f"INI ファイル書き込み失敗: {e}")

        # ポータブルモード判定（.env の MT5_PORTABLE=true または明示指定）
        is_portable = (
            params.get("portable") is True
            or os.environ.get("MT5_PORTABLE", "false").lower() == "true"
        )
        cmd = [tester_path, f"/config:{ini_path}"]
        if is_portable:
            cmd.append("/portable")
        logger.info(f"[Backtest] cmd={cmd}")

        try:
            proc = subprocess.Popen(cmd, cwd=os.path.dirname(tester_path))
        except OSError as e:
            return make_error(f"metatester64.exe 起動失敗: {e}")

        _bt_state.update({
            "process":     proc,
            "report_path": report_path,
            "ini_path":    ini_path,
            "status":      "running",
            "exit_code":   None,
        })
        logger.info(f"[Backtest] started PID={proc.pid} report={report_path}")

    return make_response({
        "status":      "started",
        "pid":         proc.pid,
        "report_path": report_path,
        "note":        "get_backtest_status で完了を確認し、完了後に get_backtest_result で結果を取得してください",
    })


def handle_get_backtest_status(params: dict) -> str:
    """実行中のバックテストの状態を返す。"""
    with _bt_lock:
        proc = _bt_state["process"]
        if proc is None:
            return make_response({"status": "idle"})

        poll = proc.poll()
        if poll is None:
            return make_response({"status": "running", "pid": proc.pid})

        # 完了 or 失敗
        new_status = "completed" if poll == 0 else "failed"
        _bt_state["status"] = new_status
        _bt_state["exit_code"] = poll
        return make_response({
            "status":      new_status,
            "exit_code":   poll,
            "report_path": _bt_state["report_path"],
        })


def handle_get_backtest_result(params: dict) -> str:
    """バックテスト結果ファイルを読み込んで主要指標を返す。"""
    with _bt_lock:
        report_path = params.get("report_path") or _bt_state.get("report_path")

    if not report_path:
        return make_error("report_path が指定されていません")

    xml_path = report_path + ".xml"
    htm_path = report_path + ".htm"

    if os.path.exists(xml_path):
        try:
            summary = _parse_backtest_xml(xml_path)
            return make_response({"format": "xml", "summary": summary, "path": xml_path})
        except ET.ParseError as e:
            return make_error(f"XML 解析エラー: {e}")

    if os.path.exists(htm_path):
        try:
            # MT5 の HTML は UTF-16 の場合がある
            for enc in ("utf-16", "utf-8", "cp932"):
                try:
                    with open(htm_path, "r", encoding=enc, errors="ignore") as f:
                        html = f.read()
                    break
                except UnicodeDecodeError:
                    continue
            summary = _parse_backtest_html(html)
            return make_response({"format": "html", "summary": summary, "path": htm_path})
        except OSError as e:
            return make_error(f"HTM 読み込みエラー: {e}")

    return make_error(
        f"レポートファイルが見つかりません: {report_path}\n"
        "バックテストが完了しているか get_backtest_status で確認してください。"
    )


# ---------------------------------------------------------------------------
# Command dispatcher
# ---------------------------------------------------------------------------
COMMANDS = {
    "get_account_info":      handle_get_account_info,
    "get_positions":         handle_get_positions,
    "get_ohlcv":             handle_get_ohlcv,
    "get_ticks":             handle_get_ticks,
    "get_ticks_hour":        handle_get_ticks_hour,
    "get_symbol_info":       handle_get_symbol_info,
    "get_history_deals":     handle_get_history_deals,
    "get_history_orders":    handle_get_history_orders,
    # バックテスト
    "run_backtest":          handle_run_backtest,
    "get_backtest_status":   handle_get_backtest_status,
    "get_backtest_result":   handle_get_backtest_result,
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
# main() が組み立てた initialize() の引数（path / login / password / server /
# portable）。再接続時も必ず同じ引数を使う。
# ⚠️ 引数なしの mt5.initialize() を呼ばないこと — レジストリ既定の端末に
#    黙って繋がり、別口座のデータを返す（口座取り違えの原因）。
INIT_KWARGS: dict = {}


def build_init_kwargs() -> dict:
    """.env から initialize() の引数を組み立てる。"""
    kwargs = {}
    mt5_path = os.environ.get("MT5_TERMINAL_PATH", "").strip()
    mt5_login = os.environ.get("MT5_LOGIN", "").strip()
    mt5_password = os.environ.get("MT5_PASSWORD", "").strip()
    mt5_server = os.environ.get("MT5_SERVER", "").strip()
    mt5_portable = os.environ.get("MT5_PORTABLE", "false").strip().lower() == "true"
    if mt5_path:
        kwargs["path"] = mt5_path
    if mt5_login:
        kwargs["login"] = int(mt5_login)
    if mt5_password:
        kwargs["password"] = mt5_password
    if mt5_server:
        kwargs["server"] = mt5_server
    if mt5_portable:
        kwargs["portable"] = True
    return kwargs


def verify_data_path() -> bool:
    """繋がった先が意図した data folder か検証する。

    MT5_PORTABLE=true のとき、接続先端末の data_path は端末のインストール
    フォルダ（= MT5_TERMINAL_PATH の親）でなければならない。
    AppData/Roaming/MetaQuotes/Terminal/<hash> に落ちていたら、非ポータブル
    起動の別プロファイルに繋がっている＝EA・ログ・MQL5/Files が全て別物に
    なるので、繋がったふりをせず失敗させる。

    ⚠️ 口座番号では判別できない（両プロファイルとも同じ口座にログインし得る）。
    判定は必ず data_path で行う。
    """
    if not INIT_KWARGS.get("portable"):
        return True
    term_path = INIT_KWARGS.get("path", "")
    if not term_path:
        return True
    expected = os.path.normcase(os.path.abspath(os.path.dirname(term_path)))
    info = mt5.terminal_info()
    if info is None:
        logger.error(f"terminal_info() returned None: {mt5.last_error()}")
        return False
    actual = os.path.normcase(os.path.abspath(info.data_path))
    if actual != expected:
        logger.error("MT5_PORTABLE=true だがポータブルでない端末に接続した。")
        logger.error(f"  expected data_path: {expected}")
        logger.error(f"  actual   data_path: {actual}")
        logger.error(
            "  → 該当端末が /portable 無しで起動している。端末を一度終了し、"
            "/portable 付きショートカットで起動し直すこと。"
        )
        return False
    return True


def connect_mt5() -> bool:
    """INIT_KWARGS で initialize し、接続先と data_path を検証してログに出す。"""
    if not mt5.initialize(**INIT_KWARGS):
        logger.error(f"MT5 initialize failed: {mt5.last_error()}")
        return False
    if not verify_data_path():
        mt5.shutdown()
        return False
    info = mt5.account_info()
    if info is None:
        logger.error(
            f"account_info() returned None（端末が未ログイン）: {mt5.last_error()}"
        )
        mt5.shutdown()
        return False
    term = mt5.terminal_info()
    data_path = term.data_path if term is not None else "?"
    logger.info(
        f"MT5 connected: account={info.login}, server={info.server}, "
        f"data_path={data_path}, portable={bool(INIT_KWARGS.get('portable'))}"
    )
    return True


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
    if not connect_mt5():
        return False
    logger.info("MT5 reconnected successfully")
    return True


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MT5 Relay Server")
    p.add_argument(
        "--port", type=int, default=None,
        help="ZMQ listen port（省略時は MT5_ZMQ_PORT env or 5580）",
    )
    p.add_argument(
        "--env", type=str, default=None,
        help=".env ファイルパス（省略時はスクリプトと同じディレクトリの .env）",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main server loop
# ---------------------------------------------------------------------------
def main():
    args = parse_args()

    # .env 読み込み（--env 指定があればそのファイルを使用）
    env_path = args.env or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    load_dotenv(env_path)
    logger.info(f"Loaded env: {env_path}")

    if mt5 is None:
        logger.error("MetaTrader5 package not available. Install on Windows VPS.")
        sys.exit(1)

    # ポート: CLI引数 > 環境変数 > デフォルト 5580
    port = args.port if args.port is not None else int(os.environ.get("MT5_ZMQ_PORT", "5580"))
    host = "127.0.0.1"

    global INIT_KWARGS
    INIT_KWARGS = build_init_kwargs()

    if not connect_mt5():
        sys.exit(1)

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
