"""
Vantage口座からXAUUSDティックデータを一括ダウンロード
対象期間: 2025-01-01 〜 2025-03-31
保存先: C:\\Users\\katun\\MQ\\tick\\Vantage\\XAUUSD\\ticks\\
"""
from __future__ import annotations

import asyncio
import csv
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import zmq
import zmq.asyncio
from dotenv import load_dotenv

# .env 読み込み
load_dotenv(Path(__file__).parent / ".env")

SYMBOL    = "XAUUSD"
START     = "2025-09-01"
END       = "2026-03-31"
BROKER    = "Vantage"
ZMQ_PORT  = 5581
ZMQ_HOST  = "localhost"
ZMQ_TIMEOUT = int(os.getenv("MT5_ZMQ_TIMEOUT", "30000"))
CACHE_ROOT  = Path(os.getenv("MT5_TICK_CACHE", r"C:\Users\katun\MQ\tick"))

TICK_FIELDS = ["time_msc", "bid", "ask", "last", "volume", "flags"]


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


class ZmqClient:
    def __init__(self, host: str, port: int, timeout: int):
        self._ctx = zmq.asyncio.Context()
        self._endpoint = f"tcp://{host}:{port}"
        self._timeout = timeout
        self._socket: zmq.asyncio.Socket | None = None

    def _ensure_socket(self) -> zmq.asyncio.Socket:
        if self._socket is None or self._socket.closed:
            self._socket = self._ctx.socket(zmq.REQ)
            self._socket.setsockopt(zmq.RCVTIMEO, self._timeout)
            self._socket.setsockopt(zmq.SNDTIMEO, self._timeout)
            self._socket.setsockopt(zmq.LINGER, 0)
            self._socket.connect(self._endpoint)
        return self._socket

    def _reset(self):
        if self._socket and not self._socket.closed:
            self._socket.close()
        self._socket = None

    async def request(self, command: str, params: dict) -> dict:
        sock = self._ensure_socket()
        msg = json.dumps({"command": command, "params": params})
        try:
            await sock.send_string(msg)
            raw = await sock.recv_string()
            return json.loads(raw)
        except zmq.Again:
            self._reset()
            return {"status": "error", "message": "timeout"}
        except zmq.ZMQError as e:
            self._reset()
            return {"status": "error", "message": str(e)}

    def close(self):
        self._reset()
        self._ctx.term()


async def download():
    client = ZmqClient(ZMQ_HOST, ZMQ_PORT, ZMQ_TIMEOUT)
    start_date = date.fromisoformat(START)
    end_date   = date.fromisoformat(END)

    total_ticks = 0
    days_ok     = 0
    days_empty  = 0
    t0 = time.time()

    print(f"[{ts()}] ダウンロード開始: {SYMBOL}  {START} 〜 {END}  ({BROKER})", flush=True)
    print(f"[{ts()}] 保存先: {CACHE_ROOT / BROKER / SYMBOL / 'ticks'}", flush=True)

    current = start_date
    while current <= end_date:
        day_ticks = []
        for hour in range(24):
            result = await client.request("get_ticks_hour", {
                "symbol": SYMBOL,
                "date": current.isoformat(),
                "hour": hour,
            })
            if result.get("status") == "ok":
                day_ticks.extend(result.get("data", []))
            await asyncio.sleep(0.05)

        if day_ticks:
            dir_path = CACHE_ROOT / BROKER / SYMBOL / "ticks"
            dir_path.mkdir(parents=True, exist_ok=True)
            file_path = dir_path / f"{current.isoformat()}.csv"
            with open(file_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=TICK_FIELDS, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(day_ticks)
            total_ticks += len(day_ticks)
            days_ok += 1
            print(f"[{ts()}]  {current}  {len(day_ticks):>7,} ticks", flush=True)
        else:
            days_empty += 1
            print(f"[{ts()}]  {current}  (データなし)", flush=True)

        current += timedelta(days=1)

    client.close()

    elapsed = time.time() - t0
    h, rem = divmod(int(elapsed), 3600)
    m, s   = divmod(rem, 60)

    ticks_dir = CACHE_ROOT / BROKER / SYMBOL / "ticks"
    size_mb = (
        sum(f.stat().st_size for f in ticks_dir.glob("*.csv")) / (1024 ** 2)
        if ticks_dir.exists() else 0.0
    )

    print(f"\n[{ts()}] ===== 完了 =====")
    print(f"  経過時間  : {h:02d}:{m:02d}:{s:02d}")
    print(f"  保存日数  : {days_ok}")
    print(f"  空白日数  : {days_empty}")
    print(f"  総ティック: {total_ticks:,}")
    print(f"  合計サイズ: {size_mb:.1f} MB")
    print(f"  保存先    : {ticks_dir}")


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(download())
