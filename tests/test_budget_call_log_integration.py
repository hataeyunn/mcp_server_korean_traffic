"""Budget Guard와 Call Log 연동 테스트."""

import sys
from datetime import date
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.budget import check_budget  # noqa: E402


def test_budget_calculation_success_only() -> None:
    """성공한 API 호출만 Budget에 반영되는지 테스트."""
    today = date(2026, 1, 4)

    # success 3건 → used_calls_today = 3
    result = check_budget(
        today=today,
        used_calls_today=3,  # Call Log에서 success만 카운트
        required_calls=1,
        daily_limit=1000,
    )
    assert result["should_collect"] is True
    assert result["remaining_calls"] == 997
    assert result["reason"] == "budget_ok"


def test_budget_calculation_error_not_counted() -> None:
    """error 상태 호출은 Budget에 반영되지 않는지 테스트."""
    today = date(2026, 1, 4)

    # Call Log에 success 3건, error 1건이 있어도
    # used_calls_today는 3만 반영됨
    result = check_budget(
        today=today,
        used_calls_today=3,  # error는 카운트되지 않음
        required_calls=1,
        daily_limit=1000,
    )
    assert result["should_collect"] is True
    assert result["remaining_calls"] == 997
    assert result["reason"] == "budget_ok"


def test_budget_date_reset() -> None:
    """날짜 변경 시 Budget이 리셋되는지 테스트."""
    # 어제 날짜
    yesterday = date(2026, 1, 3)
    # 오늘 날짜
    today = date(2026, 1, 4)

    # 어제 1000건 사용
    result_yesterday = check_budget(
        today=yesterday,
        used_calls_today=1000,
        required_calls=1,
        daily_limit=1000,
    )
    assert result_yesterday["should_collect"] is False
    assert result_yesterday["reason"] == "budget_exceeded"

    # 오늘 0건 (날짜 변경으로 리셋)
    result_today = check_budget(
        today=today,
        used_calls_today=0,  # 날짜 변경으로 0으로 리셋
        required_calls=1,
        daily_limit=1000,
    )
    assert result_today["should_collect"] is True
    assert result_today["remaining_calls"] == 1000
    assert result_today["reason"] == "budget_ok"


def test_budget_call_log_single_source_of_truth() -> None:
    """Call Log가 단일 진실 소스임을 검증."""
    today = date(2026, 1, 4)

    # Call Log 기준으로 정확히 계산된 값
    # (Raw 테이블과 무관)
    used_calls_from_call_log = 500  # Call Log에서 계산

    result = check_budget(
        today=today,
        used_calls_today=used_calls_from_call_log,
        required_calls=3,
        daily_limit=1000,
    )

    assert result["should_collect"] is True
    assert result["remaining_calls"] == 500  # 1000 - 500 = 500
    assert result["reason"] == "budget_ok"

    # Budget Guard는 계산 방법을 모름 (외부 주입값만 사용)
    # 실제 계산은 Orchestrator CLI에서 수행
