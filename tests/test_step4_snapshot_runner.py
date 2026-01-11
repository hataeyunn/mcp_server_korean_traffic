"""STEP 4 테스트: Snapshot Runner 부분 보존 로직 검증 (DI 기반)."""

import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytz

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.runner.snapshot_runner import run_snapshot_once  # noqa: E402


def test_snapshot_runner_partial_success() -> None:
    """부분 성공 시나리오 검증."""
    # Mock 설정
    mock_provider = MagicMock()
    mock_mysql_conn = MagicMock()
    mock_cursor = MagicMock()

    # 첫 번째 page: 성공 (2개 row)
    # 두 번째 page: 예외 발생
    # 세 번째 page: 성공 (1개 row)
    mock_provider.fetch_page.side_effect = [
        {"result_code": "INFO-000", "result_message": "OK", "total_count": None, "rows": [{"rowNum": "1"}, {"rowNum": "2"}]},  # page 1
        Exception("Network error"),  # page 2 실패
        {"result_code": "INFO-000", "result_message": "OK", "total_count": None, "rows": [{"rowNum": "3"}]},  # page 3
    ]

    mock_mysql_conn.cursor.return_value = mock_cursor
    mock_cursor.executemany.return_value = None
    mock_cursor.rowcount = 2  # 첫 번째와 세 번째 페이지에서 각각 2개, 1개 삽입

    # ingest_rows_page mock
    def mock_ingest_rows_page(*, mysql_conn, snapshot_id, collected_at, page_start, page_end, rows):
        if page_start == 0:
            return {"attempted_rows": 2, "inserted_rows": 2, "skipped_duplicates": 0}
        elif page_start == 2000:
            return {"attempted_rows": 1, "inserted_rows": 1, "skipped_duplicates": 0}
        return {"attempted_rows": 0, "inserted_rows": 0, "skipped_duplicates": 0}

    # clock mock
    def mock_clock() -> datetime:
        return datetime.now(pytz.timezone("Asia/Seoul"))

    with patch("ingestion.runner.snapshot_runner.ingest_rows_page") as mock_ingest:
        # ingest_rows_page mock 설정
        mock_ingest.side_effect = mock_ingest_rows_page

        # 실행 (DI 기반: provider, mysql_conn, clock 주입)
        result = run_snapshot_once(
            snapshot_id="20260104_120000",
            call_ranges=[(0, 999), (1000, 1999), (2000, 2999)],
            provider=mock_provider,
            mysql_conn=mock_mysql_conn,
            clock=mock_clock,
        )

        # 검증
        assert result["status"] == "partial", f"예상: partial, 실제: {result['status']}"
        assert len(result["pages"]) == 3, f"예상: 3개 페이지, 실제: {len(result['pages'])}"

        # 페이지 1: 성공
        assert result["pages"][0]["status"] == "ok"
        assert result["pages"][0]["attempted_rows"] == 2
        assert result["pages"][0]["inserted_rows"] == 2

        # 페이지 2: 실패
        assert result["pages"][1]["status"] == "error"
        assert result["pages"][1]["error"] is not None

        # 페이지 3: 성공
        assert result["pages"][2]["status"] == "ok"
        assert result["pages"][2]["attempted_rows"] == 1
        assert result["pages"][2]["inserted_rows"] == 1

        # 전체 합산
        assert result["attempted_total"] == 3  # 2 + 0 + 1
        assert result["inserted_total"] == 3  # 2 + 0 + 1
        assert result["errors_total"] == 1

        # commit 호출 확인 (성공한 페이지만)
        assert mock_mysql_conn.commit.call_count == 2  # 페이지 1, 3
        # rollback 호출 확인 (실패한 페이지)
        assert mock_mysql_conn.rollback.call_count == 1  # 페이지 2

        # snapshot_id가 반환 dict에 그대로 포함
        assert result["snapshot_id"] == "20260104_120000"


def test_snapshot_runner_all_success() -> None:
    """전체 성공 시나리오 검증."""
    mock_provider = MagicMock()
    mock_mysql_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_provider.fetch_page.side_effect = [
        {"result_code": "INFO-000", "result_message": "OK", "total_count": None, "rows": [{"rowNum": "1"}]},
        {"result_code": "INFO-000", "result_message": "OK", "total_count": None, "rows": [{"rowNum": "2"}]},
    ]

    mock_mysql_conn.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 1

    def mock_ingest_rows_page(*, mysql_conn, snapshot_id, collected_at, page_start, page_end, rows):
        return {"attempted_rows": 1, "inserted_rows": 1, "skipped_duplicates": 0}

    # clock mock
    def mock_clock() -> datetime:
        return datetime.now(pytz.timezone("Asia/Seoul"))

    with patch("ingestion.runner.snapshot_runner.ingest_rows_page") as mock_ingest:
        mock_ingest.side_effect = mock_ingest_rows_page

        result = run_snapshot_once(
            snapshot_id="20260104_120000",
            call_ranges=[(0, 999), (1000, 1999)],
            provider=mock_provider,
            mysql_conn=mock_mysql_conn,
            clock=mock_clock,
        )

        assert result["status"] == "ok"
        assert result["errors_total"] == 0
        assert result["attempted_total"] == 2
        assert result["inserted_total"] == 2
        assert result["snapshot_id"] == "20260104_120000"
        assert mock_mysql_conn.commit.call_count == 2  # 두 페이지 모두 성공
