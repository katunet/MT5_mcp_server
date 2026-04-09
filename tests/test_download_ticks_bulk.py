"""download_ticks_bulk のユニットテスト（ZMQ をモック）"""
import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


def make_tick(time_msc: str, bid: float, ask: float) -> dict:
    return {
        "time": time_msc,
        "time_msc": time_msc,
        "bid": bid,
        "ask": ask,
        "last": 0.0,
        "volume": 1,
        "flags": 6,
        "volume_real": 0.0,
    }


@pytest.mark.asyncio
async def test_download_ticks_bulk_creates_csv(tmp_path):
    """1日分の時間ループで CSV が生成されることを確認"""
    from server import _download_ticks_bulk_impl

    async def fake_request(command, params):
        if command == "get_ticks_hour":
            return {
                "status": "ok",
                "data": [
                    make_tick("2025-01-06T10:00:00.100000+00:00", 2650.10, 2650.35),
                    make_tick("2025-01-06T10:00:01.200000+00:00", 2650.12, 2650.37),
                ],
            }
        return {"status": "ok", "data": []}

    result = await _download_ticks_bulk_impl(
        zmq_request=fake_request,
        symbol="XAUUSD",
        start="2025-01-06",
        end="2025-01-06",
        broker="TestBroker",
        cache_root=tmp_path,
    )

    assert result["days"] == 1
    assert result["total_ticks"] == 48  # 24時間 × 2ティック
    csv_file = tmp_path / "TestBroker" / "XAUUSD" / "ticks" / "2025-01-06.csv"
    assert csv_file.exists()
    rows = list(csv.DictReader(open(csv_file)))
    assert len(rows) == 48
    assert "bid" in rows[0]
    assert "ask" in rows[0]
    assert "time_msc" in rows[0]


@pytest.mark.asyncio
async def test_download_ticks_bulk_empty_day_skipped(tmp_path):
    """ティックが 0 件の日は CSV を作成しない"""
    from server import _download_ticks_bulk_impl

    async def fake_request(command, params):
        return {"status": "ok", "data": []}

    result = await _download_ticks_bulk_impl(
        zmq_request=fake_request,
        symbol="XAUUSD",
        start="2025-01-04",
        end="2025-01-04",
        broker="TestBroker",
        cache_root=tmp_path,
    )

    assert result["days"] == 0
    assert result["total_ticks"] == 0
    csv_file = tmp_path / "TestBroker" / "XAUUSD" / "ticks" / "2025-01-04.csv"
    assert not csv_file.exists()


@pytest.mark.asyncio
async def test_download_ticks_bulk_multi_day(tmp_path):
    """複数日にまたがって CSV が日単位で保存される"""
    from server import _download_ticks_bulk_impl

    call_count = {}

    async def fake_request(command, params):
        if command == "get_ticks_hour":
            d = params["date"]
            call_count[d] = call_count.get(d, 0) + 1
            return {
                "status": "ok",
                "data": [make_tick(f"{d}T{params['hour']:02d}:00:00.000000+00:00", 2650.0, 2650.3)],
            }
        return {"status": "ok", "data": []}

    result = await _download_ticks_bulk_impl(
        zmq_request=fake_request,
        symbol="XAUUSD",
        start="2025-01-06",
        end="2025-01-08",
        broker="TestBroker",
        cache_root=tmp_path,
    )

    assert result["days"] == 3
    assert result["total_ticks"] == 72  # 3日 × 24時間 × 1ティック
    for d in ["2025-01-06", "2025-01-07", "2025-01-08"]:
        assert (tmp_path / "TestBroker" / "XAUUSD" / "ticks" / f"{d}.csv").exists()
