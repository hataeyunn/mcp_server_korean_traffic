"""Budget Guard 테스트."""

import sys
from datetime import date
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.budget import check_budget  # noqa: E402


def test_budget_ok_cases() -> None:
    """예산 충분한 경우 테스트."""
    today = date(2026, 1, 4)

    # used=0, required=3 → collect
    result = check_budget(
        today=today, used_calls_today=0, required_calls=3, daily_limit=1000
    )
    assert result["should_collect"] is True
    assert result["remaining_calls"] == 1000
    assert result["reason"] == "budget_ok"

    # used=997, required=3 → collect
    result = check_budget(
        today=today, used_calls_today=997, required_calls=3, daily_limit=1000
    )
    assert result["should_collect"] is True
    assert result["remaining_calls"] == 3
    assert result["reason"] == "budget_ok"

    # used=1000, required=0 → collect (0콜 요구는 항상 허용)
    result = check_budget(
        today=today, used_calls_today=1000, required_calls=0, daily_limit=1000
    )
    assert result["should_collect"] is True
    assert result["remaining_calls"] == 0
    assert result["reason"] == "budget_ok"


def test_budget_exceeded_cases() -> None:
    """예산 초과한 경우 테스트."""
    today = date(2026, 1, 4)

    # used=998, required=3 → block
    result = check_budget(
        today=today, used_calls_today=998, required_calls=3, daily_limit=1000
    )
    assert result["should_collect"] is False
    assert result["remaining_calls"] == 2
    assert result["reason"] == "budget_exceeded"

    # used=1000, required=1 → block
    result = check_budget(
        today=today, used_calls_today=1000, required_calls=1, daily_limit=1000
    )
    assert result["should_collect"] is False
    assert result["remaining_calls"] == 0
    assert result["reason"] == "budget_exceeded"

    # used=1200, required=1 → block (초과 사용)
    result = check_budget(
        today=today, used_calls_today=1200, required_calls=1, daily_limit=1000
    )
    assert result["should_collect"] is False
    assert result["remaining_calls"] == 0  # 음수는 0으로 간주
    assert result["reason"] == "budget_exceeded"


def test_boundary_cases() -> None:
    """경계값 테스트."""
    today = date(2026, 1, 4)

    # remaining_calls 정확히 0, required=0 → collect
    result = check_budget(
        today=today, used_calls_today=1000, required_calls=0, daily_limit=1000
    )
    assert result["should_collect"] is True
    assert result["remaining_calls"] == 0
    assert result["reason"] == "budget_ok"

    # remaining_calls 정확히 0, required=1 → block
    result = check_budget(
        today=today, used_calls_today=1000, required_calls=1, daily_limit=1000
    )
    assert result["should_collect"] is False
    assert result["remaining_calls"] == 0
    assert result["reason"] == "budget_exceeded"

    # remaining_calls 정확히 required_calls와 같을 때 → collect
    result = check_budget(
        today=today, used_calls_today=997, required_calls=3, daily_limit=1000
    )
    assert result["should_collect"] is True
    assert result["remaining_calls"] == 3
    assert result["reason"] == "budget_ok"

    # remaining_calls가 required_calls보다 1 작을 때 → block
    result = check_budget(
        today=today, used_calls_today=998, required_calls=3, daily_limit=1000
    )
    assert result["should_collect"] is False
    assert result["remaining_calls"] == 2
    assert result["reason"] == "budget_exceeded"


def test_negative_remaining_calls() -> None:
    """remaining_calls 음수 처리 테스트."""
    today = date(2026, 1, 4)

    # used_calls_today > daily_limit → remaining_calls는 0으로 간주
    result = check_budget(
        today=today, used_calls_today=1500, required_calls=1, daily_limit=1000
    )
    assert result["should_collect"] is False
    assert result["remaining_calls"] == 0  # 음수는 0으로 간주
    assert result["reason"] == "budget_exceeded"

    # used_calls_today == daily_limit + 1
    result = check_budget(
        today=today, used_calls_today=1001, required_calls=0, daily_limit=1000
    )
    assert result["should_collect"] is True  # required=0이면 항상 허용
    assert result["remaining_calls"] == 0
    assert result["reason"] == "budget_ok"
