"""Orchestrator 실행 판단 테스트."""

import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytz

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.orchestrator import run_orchestrator_once  # noqa: E402


def test_time_policy_blocked() -> None:
    """시간대 정책 차단 테스트."""
    seoul_tz = pytz.timezone("Asia/Seoul")
    now = seoul_tz.localize(datetime(2026, 1, 4, 2, 0, 0))  # 02:00 (심야)

    mock_provider = MagicMock()
    mock_mysql_conn = MagicMock()

    def mock_clock() -> datetime:
        return now

    result = run_orchestrator_once(
        now=now,
        call_ranges=[(0, 999), (1000, 1999), (2000, 2999)],
        provider=mock_provider,
        mysql_conn=mock_mysql_conn,
        clock=mock_clock,
        used_calls_today=0,
    )

    assert result["executed"] is False
    assert result["reason"] == "time_policy_blocked"
    assert result["snapshot_id"] is None


def test_budget_blocked() -> None:
    """예산 차단 테스트."""
    seoul_tz = pytz.timezone("Asia/Seoul")
    now = seoul_tz.localize(datetime(2026, 1, 4, 10, 0, 0))  # 10:00 (일반)

    mock_provider = MagicMock()
    mock_mysql_conn = MagicMock()

    def mock_clock() -> datetime:
        return now

    # get_last_snapshot_at을 patch하여 None 반환 (interval 체크 통과)
    with patch("ingestion.orchestrator.get_last_snapshot_at", return_value=None):
        # used_calls_today = 998, required_calls = 3 → 예산 부족
        result = run_orchestrator_once(
            now=now,
            call_ranges=[(0, 999), (1000, 1999), (2000, 2999)],
            provider=mock_provider,
            mysql_conn=mock_mysql_conn,
            clock=mock_clock,
            used_calls_today=998,
        )

        assert result["executed"] is False
        assert result["reason"] == "budget_blocked"
        assert result["snapshot_id"] is None


def test_executed() -> None:
    """실행 허용 테스트."""
    seoul_tz = pytz.timezone("Asia/Seoul")
    now = seoul_tz.localize(datetime(2026, 1, 4, 10, 0, 0))  # 10:00 (일반)

    mock_provider = MagicMock()
    mock_mysql_conn = MagicMock()

    def mock_clock() -> datetime:
        return now

    # get_last_snapshot_at을 patch하여 None 반환 (interval 체크 통과)
    with patch("ingestion.orchestrator.get_last_snapshot_at", return_value=None), patch(
        "ingestion.orchestrator.run_snapshot_once"
    ) as mock_runner:
        result = run_orchestrator_once(
            now=now,
            call_ranges=[(0, 999), (1000, 1999), (2000, 2999)],
            provider=mock_provider,
            mysql_conn=mock_mysql_conn,
            clock=mock_clock,
            used_calls_today=0,
        )

        # 검증
        assert result["executed"] is True
        assert result["reason"] == "executed"
        # snapshot_id는 now.strftime("%Y%m%d_%H%M%S") 형식으로 생성됨
        assert result["snapshot_id"] == "20260104_100000"

        # Runner 호출 확인
        mock_runner.assert_called_once()
        call_args = mock_runner.call_args
        assert call_args.kwargs["snapshot_id"] == "20260104_100000"
        assert call_args.kwargs["call_ranges"] == [(0, 999), (1000, 1999), (2000, 2999)]
        assert call_args.kwargs["provider"] == mock_provider
        assert call_args.kwargs["mysql_conn"] == mock_mysql_conn
        assert call_args.kwargs["clock"] == mock_clock


def test_executed_morning_rush_hour() -> None:
    """출근 시간대 실행 허용 테스트."""
    seoul_tz = pytz.timezone("Asia/Seoul")
    now = seoul_tz.localize(datetime(2026, 1, 4, 8, 0, 0))  # 08:00 (출근)

    mock_provider = MagicMock()
    mock_mysql_conn = MagicMock()

    def mock_clock() -> datetime:
        return now

    # get_last_snapshot_at을 patch하여 None 반환 (interval 체크 통과)
    with patch("ingestion.orchestrator.get_last_snapshot_at", return_value=None), patch(
        "ingestion.orchestrator.run_snapshot_once"
    ) as mock_runner:
        result = run_orchestrator_once(
            now=now,
            call_ranges=[(0, 999), (1000, 1999)],
            provider=mock_provider,
            mysql_conn=mock_mysql_conn,
            clock=mock_clock,
            used_calls_today=500,
        )

        assert result["executed"] is True
        assert result["reason"] == "executed"
        # snapshot_id는 now.strftime("%Y%m%d_%H%M%S") 형식으로 생성됨
        assert result["snapshot_id"] == "20260104_080000"
        mock_runner.assert_called_once()


def test_interval_not_elapsed_normal_time() -> None:
    """일반 시간대(15분)에서 1분 간격 실행 시 두 번째 실행이 SKIP 되는 테스트."""
    seoul_tz = pytz.timezone("Asia/Seoul")
    # 첫 번째 실행: 10:00
    first_run = seoul_tz.localize(datetime(2026, 1, 4, 10, 0, 0))
    # 두 번째 실행: 10:01 (1분 후, 15분 간격 미충족)
    second_run = seoul_tz.localize(datetime(2026, 1, 4, 10, 1, 0))

    mock_provider = MagicMock()
    mock_mysql_conn = MagicMock()

    def mock_clock() -> datetime:
        return second_run

    # get_last_snapshot_at이 첫 번째 실행 시각을 반환
    with patch(
        "ingestion.orchestrator.get_last_snapshot_at", return_value=first_run
    ), patch("ingestion.orchestrator.run_snapshot_once") as mock_runner:
        result = run_orchestrator_once(
            now=second_run,
            call_ranges=[(0, 999), (1000, 1999), (2000, 2999)],
            provider=mock_provider,
            mysql_conn=mock_mysql_conn,
            clock=mock_clock,
            used_calls_today=0,
        )

        # 검증: 실행 차단됨
        assert result["executed"] is False
        assert result["reason"] == "interval_not_elapsed"
        assert result["snapshot_id"] is None
        assert result["interval_seconds"] == 900  # 일반 시간대는 15분(900초)
        assert result["elapsed_seconds"] == 60.0  # 1분 경과
        assert result["last_snapshot_at"] == first_run

        # Runner는 호출되지 않아야 함
        mock_runner.assert_not_called()


def test_interval_not_elapsed_commute_time() -> None:
    """출퇴근 시간대(2분)에서 2분 미만 실행 시 SKIP 되는 테스트."""
    seoul_tz = pytz.timezone("Asia/Seoul")
    # 첫 번째 실행: 08:00 (출근 시간대)
    first_run = seoul_tz.localize(datetime(2026, 1, 4, 8, 0, 0))
    # 두 번째 실행: 08:01 (1분 후, 2분 간격 미충족)
    second_run = seoul_tz.localize(datetime(2026, 1, 4, 8, 1, 0))

    mock_provider = MagicMock()
    mock_mysql_conn = MagicMock()

    def mock_clock() -> datetime:
        return second_run

    # get_last_snapshot_at이 첫 번째 실행 시각을 반환
    with patch(
        "ingestion.orchestrator.get_last_snapshot_at", return_value=first_run
    ), patch("ingestion.orchestrator.run_snapshot_once") as mock_runner:
        result = run_orchestrator_once(
            now=second_run,
            call_ranges=[(0, 999), (1000, 1999)],
            provider=mock_provider,
            mysql_conn=mock_mysql_conn,
            clock=mock_clock,
            used_calls_today=0,
        )

        # 검증: 실행 차단됨
        assert result["executed"] is False
        assert result["reason"] == "interval_not_elapsed"
        assert result["snapshot_id"] is None
        assert result["interval_seconds"] == 120  # 출퇴근 시간대는 2분(120초)
        assert result["elapsed_seconds"] == 60.0  # 1분 경과
        assert result["last_snapshot_at"] == first_run

        # Runner는 호출되지 않아야 함
        mock_runner.assert_not_called()
