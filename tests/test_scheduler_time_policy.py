"""Scheduler 시간대 정책 테스트.

이 테스트는 Scheduler 정책의 단일 진실 역할을 합니다.
구현 상세가 아니라 "정책 값"을 검증합니다.
"""

import sys
from datetime import datetime
from pathlib import Path

import pytz

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.scheduler import (  # noqa: E402
    COMMUTE_INTERVAL_SECONDS,
    NORMAL_INTERVAL_SECONDS,
    decide_collection,
)


def test_commute_time_interval_is_120_seconds() -> None:
    """출근 시간대 수집 간격이 120초(2분)인지 검증."""
    seoul_tz = pytz.timezone("Asia/Seoul")

    # 07:00:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 7, 0, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is True
    assert result["interval_seconds"] == COMMUTE_INTERVAL_SECONDS
    assert result["interval_seconds"] == 120
    assert result["time_bucket"] == "morning"

    # 08:15:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 8, 15, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is True
    assert result["interval_seconds"] == COMMUTE_INTERVAL_SECONDS
    assert result["interval_seconds"] == 120
    assert result["time_bucket"] == "morning"

    # 09:30:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 9, 30, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is True
    assert result["interval_seconds"] == COMMUTE_INTERVAL_SECONDS
    assert result["interval_seconds"] == 120
    assert result["time_bucket"] == "morning"


def test_evening_time_interval_is_120_seconds() -> None:
    """퇴근 시간대 수집 간격이 120초(2분)인지 검증."""
    seoul_tz = pytz.timezone("Asia/Seoul")

    # 17:30:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 17, 30, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is True
    assert result["interval_seconds"] == COMMUTE_INTERVAL_SECONDS
    assert result["interval_seconds"] == 120
    assert result["time_bucket"] == "evening"

    # 19:00:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 19, 0, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is True
    assert result["interval_seconds"] == COMMUTE_INTERVAL_SECONDS
    assert result["interval_seconds"] == 120
    assert result["time_bucket"] == "evening"

    # 20:00:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 20, 0, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is True
    assert result["interval_seconds"] == COMMUTE_INTERVAL_SECONDS
    assert result["interval_seconds"] == 120
    assert result["time_bucket"] == "evening"


def test_normal_time_interval_is_900_seconds() -> None:
    """일반 시간대 수집 간격이 900초(15분)인지 검증."""
    seoul_tz = pytz.timezone("Asia/Seoul")

    # 06:00:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 6, 0, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is True
    assert result["interval_seconds"] == NORMAL_INTERVAL_SECONDS
    assert result["interval_seconds"] == 900
    assert result["time_bucket"] == "normal"

    # 10:00:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 10, 0, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is True
    assert result["interval_seconds"] == NORMAL_INTERVAL_SECONDS
    assert result["interval_seconds"] == 900
    assert result["time_bucket"] == "normal"

    # 23:00:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 23, 0, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is True
    assert result["interval_seconds"] == NORMAL_INTERVAL_SECONDS
    assert result["interval_seconds"] == 900
    assert result["time_bucket"] == "normal"

    # 00:15:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 0, 15, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is True
    assert result["interval_seconds"] == NORMAL_INTERVAL_SECONDS
    assert result["interval_seconds"] == 900
    assert result["time_bucket"] == "normal"


def test_night_time_should_not_collect() -> None:
    """심야 시간대는 수집하지 않는지 검증."""
    seoul_tz = pytz.timezone("Asia/Seoul")

    # 00:30:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 0, 30, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is False
    assert result["interval_seconds"] > 0
    assert result["time_bucket"] == "night"
    # 05:00:01까지의 초 계산 확인
    expected_seconds = (5 * 3600 + 1) - (30 * 60)  # 05:00:01 - 00:30:00
    assert result["interval_seconds"] == expected_seconds

    # 02:00:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 2, 0, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is False
    assert result["interval_seconds"] > 0
    assert result["time_bucket"] == "night"
    # 05:00:01까지의 초 계산 확인
    expected_seconds = (5 * 3600 + 1) - (2 * 3600)  # 05:00:01 - 02:00:00
    assert result["interval_seconds"] == expected_seconds

    # 05:00:00
    now = seoul_tz.localize(datetime(2026, 1, 4, 5, 0, 0))
    result = decide_collection(now=now)
    assert result["should_collect"] is False
    assert result["interval_seconds"] > 0
    assert result["time_bucket"] == "night"
    # 05:00:01까지의 초 계산 확인
    expected_seconds = 1  # 05:00:01 - 05:00:00
    assert result["interval_seconds"] == expected_seconds


def test_time_boundary_transitions() -> None:
    """시간대 경계 전환 검증."""
    seoul_tz = pytz.timezone("Asia/Seoul")

    # 09:30:01 → 일반 (출근 시간대 종료)
    now = seoul_tz.localize(datetime(2026, 1, 4, 9, 30, 1))
    result = decide_collection(now=now)
    assert result["time_bucket"] == "normal"
    assert result["should_collect"] is True
    assert result["interval_seconds"] == NORMAL_INTERVAL_SECONDS
    assert result["interval_seconds"] == 900

    # 20:00:01 → 일반 (퇴근 시간대 종료)
    now = seoul_tz.localize(datetime(2026, 1, 4, 20, 0, 1))
    result = decide_collection(now=now)
    assert result["time_bucket"] == "normal"
    assert result["should_collect"] is True
    assert result["interval_seconds"] == NORMAL_INTERVAL_SECONDS
    assert result["interval_seconds"] == 900

    # 00:29:59 → 일반 (심야 시작 전)
    now = seoul_tz.localize(datetime(2026, 1, 4, 0, 29, 59))
    result = decide_collection(now=now)
    assert result["time_bucket"] == "normal"
    assert result["should_collect"] is True
    assert result["interval_seconds"] == NORMAL_INTERVAL_SECONDS
    assert result["interval_seconds"] == 900

    # 05:00:01 → 일반 (심야 종료 후)
    now = seoul_tz.localize(datetime(2026, 1, 4, 5, 0, 1))
    result = decide_collection(now=now)
    assert result["time_bucket"] == "normal"
    assert result["should_collect"] is True
    assert result["interval_seconds"] == NORMAL_INTERVAL_SECONDS
    assert result["interval_seconds"] == 900
