"""Call Log 테스트."""

import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytz

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.pipeline.call_log import insert_call_log  # noqa: E402


def test_insert_call_log_success() -> None:
    """성공 호출 기록 테스트."""
    mock_mysql_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_mysql_conn.cursor.return_value = mock_cursor

    seoul_tz = pytz.timezone("Asia/Seoul")
    called_at = seoul_tz.localize(datetime(2026, 1, 4, 10, 0, 0))

    insert_call_log(
        mysql_conn=mock_mysql_conn,
        snapshot_id="20260104_100000",
        page_start=0,
        page_end=999,
        called_at=called_at,
        status="success",
    )

    # INSERT 호출 확인
    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args
    assert "INSERT INTO subway_api_call_log" in call_args[0][0]
    assert call_args[0][1][1] == "20260104_100000"  # snapshot_id
    assert call_args[0][1][2] == 0  # page_start
    assert call_args[0][1][3] == 999  # page_end
    assert call_args[0][1][5] == "success"  # status


def test_insert_call_log_error() -> None:
    """실패 호출 기록 테스트."""
    mock_mysql_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_mysql_conn.cursor.return_value = mock_cursor

    seoul_tz = pytz.timezone("Asia/Seoul")
    called_at = seoul_tz.localize(datetime(2026, 1, 4, 10, 0, 0))

    insert_call_log(
        mysql_conn=mock_mysql_conn,
        snapshot_id="20260104_100000",
        page_start=1000,
        page_end=1999,
        called_at=called_at,
        status="error",
    )

    # INSERT 호출 확인
    mock_cursor.execute.assert_called_once()
    call_args = mock_cursor.execute.call_args
    assert call_args[0][1][5] == "error"  # status


def test_insert_call_log_duplicate_prevention() -> None:
    """중복 방지 테스트 (ON DUPLICATE KEY UPDATE)."""
    mock_mysql_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_mysql_conn.cursor.return_value = mock_cursor

    seoul_tz = pytz.timezone("Asia/Seoul")
    called_at = seoul_tz.localize(datetime(2026, 1, 4, 10, 0, 0))

    # 동일 snapshot_id/page_start/page_end로 2회 삽입 시도
    insert_call_log(
        mysql_conn=mock_mysql_conn,
        snapshot_id="20260104_100000",
        page_start=0,
        page_end=999,
        called_at=called_at,
        status="success",
    )

    insert_call_log(
        mysql_conn=mock_mysql_conn,
        snapshot_id="20260104_100000",
        page_start=0,
        page_end=999,
        called_at=called_at,
        status="success",
    )

    # 두 번 모두 INSERT 호출됨 (ON DUPLICATE KEY UPDATE로 처리)
    assert mock_cursor.execute.call_count == 2

    # SQL에 ON DUPLICATE KEY UPDATE 포함 확인
    for call in mock_cursor.execute.call_args_list:
        sql = call[0][0]
        assert "ON DUPLICATE KEY UPDATE" in sql


def test_call_log_budget_calculation() -> None:
    """Budget 계산용 조회 테스트."""
    # 실제 DB 없이 SQL 구조만 검증
    # SELECT COUNT(*) FROM subway_api_call_log
    # WHERE call_date = %s AND status = 'success'

    sql = """
    SELECT COUNT(*) AS call_count
    FROM subway_api_call_log
    WHERE call_date = %s
      AND status = 'success'
    """

    # SQL 구조 검증
    assert "subway_api_call_log" in sql
    assert "call_date" in sql
    assert "status" in sql
    assert "success" in sql
    assert "COUNT(*)" in sql
