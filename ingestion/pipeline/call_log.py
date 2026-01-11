"""Call Log: API 호출 기록 (단일 진실 소스).

STEP 8:
본 모듈은 API 호출 사실을 기록하는 Call Log 역할만 수행한다.
Budget 계산은 STEP 8.5에서 본 테이블을 단일 진실 소스로 전환한다.
"""

from datetime import datetime
from typing import Any


def insert_call_log(
    *,
    mysql_conn: Any,
    snapshot_id: str,
    page_start: int,
    page_end: int,
    called_at: datetime,
    status: str,  # "success" | "error"
) -> None:
    """
    Call Log에 API 호출 기록을 삽입.

    Args:
        mysql_conn: MySQL 연결 객체 (외부 주입)
        snapshot_id: 스냅샷 ID
        page_start: 페이지 시작 범위
        page_end: 페이지 종료 범위
        called_at: 실제 API 호출 시각 (Asia/Seoul)
        status: 호출 결과 ("success" 또는 "error")

    주의:
        - commit/rollback은 호출자(Runner)가 제어
        - UNIQUE(snapshot_id, page_start, page_end)로 중복 기록 방지
        - 중복 시 예외 없이 무시되도록 처리 (ON DUPLICATE KEY UPDATE)
    """
    # call_date는 called_at의 날짜 부분 (Asia/Seoul 기준)
    call_date = called_at.date()

    cursor = mysql_conn.cursor()
    try:
        sql = """
        INSERT INTO subway_api_call_log
        (call_date, snapshot_id, page_start, page_end, called_at, status)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        status = VALUES(status),
        called_at = VALUES(called_at)
        """
        cursor.execute(
            sql,
            (call_date, snapshot_id, page_start, page_end, called_at, status),
        )
        # commit/rollback은 호출자가 제어
    finally:
        cursor.close()
