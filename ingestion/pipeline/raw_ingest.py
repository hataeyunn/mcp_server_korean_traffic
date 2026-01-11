"""Raw 데이터 적재 파이프라인."""

import hashlib
import json
from datetime import datetime
from typing import Any

import pytz


def compute_payload_hash(raw_payload: dict[str, Any]) -> str:
    """
    raw_payload의 canonical JSON 해시를 계산.

    Args:
        raw_payload: XML <row>를 파싱한 dict

    Returns:
        SHA-256 hex digest (64자)

    규칙:
    1) dict를 key 기준 오름차순 정렬
    2) JSON 직렬화 (ensure_ascii=False, separators=(",", ":"))
    3) SHA-256 해시 계산
    4) hex digest 반환
    """
    # key 기준 오름차순 정렬된 dict 생성
    sorted_payload = dict(sorted(raw_payload.items()))

    # JSON 직렬화 (canonical form)
    json_str = json.dumps(sorted_payload, ensure_ascii=False, separators=(",", ":"))

    # SHA-256 해시 계산
    hash_obj = hashlib.sha256(json_str.encode("utf-8"))
    return hash_obj.hexdigest()


def generate_snapshot_id() -> str:
    """
    스냅샷 ID를 생성.

    Returns:
        YYYYMMDD_HHMMSS 형식의 문자열 (Asia/Seoul 타임존)
    """
    seoul_tz = pytz.timezone("Asia/Seoul")
    now = datetime.now(seoul_tz)
    return now.strftime("%Y%m%d_%H%M%S")


def ingest_provider_result(
    provider_result: Any, snapshot_id: str, mysql_conn: Any
) -> dict[str, int]:
    """
    Provider 결과를 Raw 테이블에 적재.

    Args:
        provider_result: ProviderResult 객체
        snapshot_id: 스냅샷 ID
        mysql_conn: MySQL 연결 객체 (cursor() 메서드 보유)

    Returns:
        {
            "attempted_rows": int,
            "inserted_rows": int,
            "skipped_duplicates": int
        }
    """
    collected_at = datetime.now(pytz.timezone("Asia/Seoul"))

    # INSERT 대상 row 리스트 생성
    insert_rows = []

    for page in provider_result.pages:
        for row in page.rows:
            # payload_hash 계산
            payload_hash = compute_payload_hash(row)

            # INSERT 값 준비
            insert_values = (
                snapshot_id,
                collected_at,
                page.start,
                page.end,
                json.dumps(row, ensure_ascii=False),  # raw_payload JSON
                payload_hash,
            )
            insert_rows.append(insert_values)

    if not insert_rows:
        return {
            "attempted_rows": 0,
            "inserted_rows": 0,
            "skipped_duplicates": 0,
        }

    # Bulk INSERT 수행
    cursor = mysql_conn.cursor()

    sql = """
    INSERT INTO subway_arrival_raw
    (snapshot_id, collected_at, page_start, page_end, raw_payload, payload_hash)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    payload_hash = payload_hash
    """

    attempted_rows = len(insert_rows)
    inserted_rows = 0
    skipped_duplicates = 0

    try:
        # executemany로 bulk insert
        cursor.executemany(sql, insert_rows)
        inserted_rows = cursor.rowcount
        skipped_duplicates = attempted_rows - inserted_rows
        mysql_conn.commit()
    except Exception as e:
        mysql_conn.rollback()
        raise RuntimeError(f"Raw 데이터 적재 실패: {e}") from e
    finally:
        cursor.close()

    return {
        "attempted_rows": attempted_rows,
        "inserted_rows": inserted_rows,
        "skipped_duplicates": skipped_duplicates,
    }


def ingest_rows_page(
    *,
    mysql_conn: Any,
    snapshot_id: str,
    collected_at: datetime,
    page_start: int,
    page_end: int,
    rows: list[dict[str, Any]],
) -> dict[str, int]:
    """
    단일 페이지의 rows를 Raw 테이블에 적재.

    Args:
        mysql_conn: MySQL 연결 객체 (외부 주입, connect/close 금지)
        snapshot_id: 스냅샷 ID
        collected_at: 수집 시각
        page_start: 페이지 시작 범위
        page_end: 페이지 종료 범위
        rows: 적재할 row 리스트

    Returns:
        {
            "attempted_rows": int,
            "inserted_rows": int,
            "skipped_duplicates": int
        }

    주의:
        - commit/rollback은 이 함수에서 하지 않는다 (Runner가 페이지 단위로 수행)
        - mysql_conn은 외부에서 주입받으며, 이 함수에서 생성/종료하지 않는다
    """

    # INSERT 대상 row 리스트 생성
    insert_rows = []

    for row in rows:
        # payload_hash 계산
        payload_hash = compute_payload_hash(row)

        # INSERT 값 준비
        insert_values = (
            snapshot_id,
            collected_at,
            page_start,
            page_end,
            json.dumps(row, ensure_ascii=False),  # raw_payload JSON
            payload_hash,
        )
        insert_rows.append(insert_values)

    if not insert_rows:
        return {
            "attempted_rows": 0,
            "inserted_rows": 0,
            "skipped_duplicates": 0,
        }

    # Bulk INSERT 수행
    cursor = mysql_conn.cursor()

    sql = """
    INSERT INTO subway_arrival_raw
    (snapshot_id, collected_at, page_start, page_end, raw_payload, payload_hash)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    payload_hash = payload_hash
    """

    attempted_rows = len(insert_rows)
    inserted_rows = 0
    skipped_duplicates = 0

    try:
        # executemany로 bulk insert
        cursor.executemany(sql, insert_rows)
        inserted_rows = cursor.rowcount
        skipped_duplicates = attempted_rows - inserted_rows
        # commit/rollback은 Runner가 페이지 단위로 수행
    except Exception as e:
        raise RuntimeError(f"Raw 데이터 적재 실패: {e}") from e
    finally:
        cursor.close()

    return {
        "attempted_rows": attempted_rows,
        "inserted_rows": inserted_rows,
        "skipped_duplicates": skipped_duplicates,
    }

