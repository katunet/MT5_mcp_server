"""
Vantage MT5リレー診断スクリプト
- 接続確認
- シンボル名確認
- 直近ティック取得テスト
"""
import asyncio
import json
import os
from dotenv import load_dotenv
from pathlib import Path

import zmq
import zmq.asyncio

load_dotenv(Path(__file__).parent / ".env")

ZMQ_PORT    = 5581
ZMQ_HOST    = "localhost"
ZMQ_TIMEOUT = 10000  # 10秒

async def req(ctx, endpoint, command, params):
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.RCVTIMEO, ZMQ_TIMEOUT)
    sock.setsockopt(zmq.SNDTIMEO, ZMQ_TIMEOUT)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(endpoint)
    msg = json.dumps({"command": command, "params": params})
    try:
        await sock.send_string(msg)
        raw = await sock.recv_string()
        return json.loads(raw)
    except zmq.Again:
        return {"status": "error", "message": "timeout"}
    finally:
        sock.close()

async def main():
    endpoint = f"tcp://{ZMQ_HOST}:{ZMQ_PORT}"
    ctx = zmq.asyncio.Context()

    print(f"=== Vantage MT5リレー診断 ({endpoint}) ===\n")

    # 1. アカウント情報
    print("[1] get_account_info ...")
    r = await req(ctx, endpoint, "get_account_info", {})
    print(json.dumps(r, indent=2, ensure_ascii=False))
    print()

    # 2. XAUUSD シンボル情報
    print("[2] get_symbol_info (XAUUSD) ...")
    r = await req(ctx, endpoint, "get_symbol_info", {"symbol": "XAUUSD"})
    print(json.dumps(r, indent=2, ensure_ascii=False))
    print()

    # 3. XAUUSDm シンボル情報（ブローカー名バリアント）
    print("[3] get_symbol_info (XAUUSDm) ...")
    r = await req(ctx, endpoint, "get_symbol_info", {"symbol": "XAUUSDm"})
    print(json.dumps(r, indent=2, ensure_ascii=False))
    print()

    # 4. 直近ティック（2025-01-06 月曜 08:00 UTC）
    print("[4] get_ticks_hour (XAUUSD, 2025-01-06, hour=8) ...")
    r = await req(ctx, endpoint, "get_ticks_hour", {"symbol": "XAUUSD", "date": "2025-01-06", "hour": 8})
    data = r.get("data", [])
    if r.get("status") == "ok":
        print(f"  -> {len(data)} ticks")
        if data:
            print(f"  先頭: {data[0]}")
    else:
        print(f"  -> {r}")
    print()

    ctx.term()

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
