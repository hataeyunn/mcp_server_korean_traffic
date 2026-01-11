"""Snapshot Runner: 단발 실행 단위 오케스트레이션.

STEP 8:
본 모듈은 Provider 호출 시 Call Log를 기록한다.
Budget 계산은 STEP 8.5에서 Call Log 테이블을 단일 진실 소스로 전환한다.
"""

import json
import logging
from datetime import datetime
from typing import Any, Callable

from ingestion.pipeline.call_log import insert_call_log
from ingestion.pipeline.raw_ingest import ingest_rows_page

logger = logging.getLogger(__name__)


def run_snapshot_once(
    *,
    snapshot_id: str,
    call_ranges: list[tuple[int, int]] | None = None,
    provider: Any,  # SeoulSubwayArrivalProvider
    mysql_conn: Any,  # 외부 주입
    clock: Callable[[], datetime],  # callable returning datetime (DI)
) -> dict[str, Any]:
    """
    스냅샷 1회 실행.

    Args:
        snapshot_id: 스냅샷 ID (외부에서 주입)
        call_ranges: 호출할 범위 리스트 (외부에서 주입, None이면 동적 결정)
        provider: SeoulSubwayArrivalProvider 인스턴스 (외부에서 주입)
        mysql_conn: MySQL 연결 객체 (외부에서 주입, connect/close 금지)
        clock: 현재 시각을 반환하는 callable (DI)

    Returns:
        {
            "snapshot_id": str,
            "ranges": list[tuple[int, int]],
            "pages": list[dict],
            "attempted_total": int,
            "inserted_total": int,
            "duplicates_total": int,
            "errors_total": int,
            "status": "ok" | "partial" | "error"
        }

    동작 방식:
        - call_ranges가 None이면 동적으로 결정 (정책 판단):
          1. page 1 (0~999) 단독 호출
          2. totalCount 파싱 및 decided_page_count 결정 (3 or 4)
          3. call_ranges 구성: page 2 이후만 포함
        - call_ranges가 제공되면 외부 명시 호출:
          - page 1은 호출하지 않음
          - totalCount 판단 없음
          - decided_page_count = None
    """
    # snapshot 기준 collected_at 결정 (모든 페이지에 동일하게 사용)
    snapshot_collected_at = clock()
    snapshot_time = snapshot_collected_at.isoformat()

    # 페이지별 결과 저장
    pages_result = []

    # 동적 호출 횟수 결정
    if call_ranges is None:
        # [1단계] page 1 단독 호출 (offset 0~999)
        first_page_start = 0
        first_page_end = 999
        page_1_success = False
        total_count = None
        decided_page_count = 3  # 기본값 (fallback)

        try:
            page_data = provider.fetch_page(first_page_start, first_page_end)
            total_count = page_data.get("total_count")
            rows = page_data["rows"]

            # Call Log 기록
            called_at = clock()
            insert_call_log(
                mysql_conn=mysql_conn,
                snapshot_id=snapshot_id,
                page_start=first_page_start,
                page_end=first_page_end,
                called_at=called_at,
                status="success",
            )

            # 첫 페이지 데이터 적재
            ingest_result = ingest_rows_page(
                mysql_conn=mysql_conn,
                snapshot_id=snapshot_id,
                collected_at=snapshot_collected_at,
                page_start=first_page_start,
                page_end=first_page_end,
                rows=rows,
            )
            mysql_conn.commit()

            page_result = {
                "start": first_page_start,
                "end": first_page_end,
                "status": "ok",
                "attempted_rows": len(rows),
                "inserted_rows": ingest_result["inserted_rows"],
                "skipped_duplicates": ingest_result["skipped_duplicates"],
                "error": None,
            }
            pages_result.append(page_result)
            page_1_success = True

            # [정책 결정] totalCount 기반 호출 횟수 결정 (단 한 번, 절대 변경 금지)
            if total_count is not None and total_count > 3000:
                decided_page_count = 4
            else:
                # totalCount가 없거나 ≤ 3000인 경우 3회 호출
                decided_page_count = 3

        except Exception as e:
            # 첫 페이지 호출 실패 시 Call Log 기록
            try:
                called_at = clock()
                insert_call_log(
                    mysql_conn=mysql_conn,
                    snapshot_id=snapshot_id,
                    page_start=first_page_start,
                    page_end=first_page_end,
                    called_at=called_at,
                    status="error",
                )
            except Exception:
                pass

            mysql_conn.rollback()
            # 첫 페이지 실패 시 기본 3회 호출로 fallback
            decided_page_count = 3
            total_count = None

            page_result = {
                "start": first_page_start,
                "end": first_page_end,
                "status": "error",
                "attempted_rows": 0,
                "inserted_rows": 0,
                "skipped_duplicates": 0,
                "error": str(e),
            }
            pages_result.append(page_result)

        # [2단계] call_ranges 구성: page 1은 제외, 앞으로 호출할 페이지만 포함
        if decided_page_count == 4:
            call_ranges = [
                (1000, 1999),   # page 2
                (2000, 2999),   # page 3
                (3000, 3999),   # page 4
            ]
        else:
            call_ranges = [
                (1000, 1999),   # page 2
                (2000, 2999),   # page 3
            ]

        remaining_ranges = call_ranges
    else:
        # call_ranges가 제공된 경우 (외부 명시 호출)
        # [규약] call_ranges에는 page 2 이후만 포함, page 1은 호출하지 않음
        # [정책 판단 없음] totalCount 판단을 수행하지 않음
        # [decided_page_count] 정책 판단이 아니므로 None
        remaining_ranges = call_ranges
        total_count = None
        decided_page_count = None  # 외부 명시 호출이므로 정책 판단 값 없음
        page_4_attempted = False
        page_4_success = False

    # [3단계] 나머지 페이지 순차 실행 (페이지 단위 원자성)
    # page_4_attempted와 page_4_success는 call_ranges=None인 경우 이미 초기화됨
    # call_ranges가 제공된 경우에도 초기화되어 있음
    for start, end in remaining_ranges:
        # page 4 호출 시도 여부 추적
        if start == 3000 and end == 3999:
            page_4_attempted = True
        page_result = {
            "start": start,
            "end": end,
            "status": "ok",
            "attempted_rows": 0,
            "inserted_rows": 0,
            "skipped_duplicates": 0,
            "error": None,
        }

        try:
            # Provider로 해당 페이지 호출
            page_data = provider.fetch_page(start, end)
            rows = page_data["rows"]
            attempted_rows = len(rows)

            # Call Log 기록 (API 호출 성공)
            called_at = clock()  # 실제 API 호출 시각
            insert_call_log(
                mysql_conn=mysql_conn,
                snapshot_id=snapshot_id,
                page_start=start,
                page_end=end,
                called_at=called_at,
                status="success",
            )

            # 해당 페이지의 rows만 Raw ingest 함수에 넘겨 DB 적재
            ingest_result = ingest_rows_page(
                mysql_conn=mysql_conn,
                snapshot_id=snapshot_id,
                collected_at=snapshot_collected_at,
                page_start=start,
                page_end=end,
                rows=rows,
            )

            page_result["attempted_rows"] = attempted_rows
            page_result["inserted_rows"] = ingest_result["inserted_rows"]
            page_result["skipped_duplicates"] = ingest_result["skipped_duplicates"]
            page_result["status"] = "ok"

            # page 4 호출 성공 여부 추적
            if start == 3000 and end == 3999:
                page_4_success = True

            # 페이지 성공 시 commit
            mysql_conn.commit()

        except Exception as e:
            # API 호출 실패 시 Call Log 기록 (error)
            try:
                called_at = clock()  # 실제 API 호출 시도 시각
                insert_call_log(
                    mysql_conn=mysql_conn,
                    snapshot_id=snapshot_id,
                    page_start=start,
                    page_end=end,
                    called_at=called_at,
                    status="error",
                )
            except Exception:
                # Call Log 기록 실패는 무시 (원래 예외가 더 중요)
                pass

            # 페이지 실패 시 rollback (그 페이지 insert만 롤백)
            mysql_conn.rollback()
            page_result["status"] = "error"
            page_result["error"] = str(e)

            # page 4 실패 시 로그만 남기고 계속 진행 (snapshot은 page 1~3 결과로 생성)
            if start == 3000 and end == 3999:
                logger.warning(
                    "page 4 호출 실패, page 1~3 결과만으로 snapshot 생성",
                    extra={
                        "structured_data": json.dumps({
                            "snapshot_time": snapshot_time,
                            "error": str(e),
                        })
                    },
                )

        pages_result.append(page_result)

    # 최종 summary 계산
    attempted_total = sum(p["attempted_rows"] for p in pages_result)
    inserted_total = sum(p["inserted_rows"] for p in pages_result)
    duplicates_total = sum(p["skipped_duplicates"] for p in pages_result)
    errors_total = sum(1 for p in pages_result if p["status"] == "error")

    # status 결정
    if errors_total == 0:
        status = "ok"
    elif errors_total == len(pages_result):
        status = "error"
    else:
        status = "partial"

    # [호출 수 개념 분리]
    # attempted_pages: 실제 호출을 시도한 페이지 수 (page 1 포함)
    attempted_pages = len(pages_result)
    # success_pages: HTTP 성공 및 파싱 성공한 호출 수
    success_pages = len([p for p in pages_result if p["status"] == "ok"])

    # 최종 구조화 로그 기록
    # call_ranges 제공 여부에 따라 로그 필드 분리
    # [규칙] decided_page_count는 재계산하지 않음, 정의된 값을 그대로 사용
    if call_ranges is None:
        # 정책 판단 실행: decided_page_count, totalCount 포함
        log_data = {
            "snapshot_time": snapshot_time,
            "totalCount": total_count,
            "decided_page_count": decided_page_count,  # 3 or 4
            "attempted_pages": attempted_pages,
            "success_pages": success_pages,
            "page_4_attempted": page_4_attempted if "page_4_attempted" in locals() else False,
            "page_4_success": page_4_success if "page_4_success" in locals() else False,
        }
    else:
        # 외부 명시 호출: decided_page_count, totalCount 절대 로그 금지
        log_data = {
            "snapshot_time": snapshot_time,
            "attempted_pages": attempted_pages,
            "success_pages": success_pages,
            "page_4_attempted": page_4_attempted if "page_4_attempted" in locals() else False,
            "page_4_success": page_4_success if "page_4_success" in locals() else False,
        }
    
    logger.info("API 호출 완료", extra={"structured_data": json.dumps(log_data)})

    return {
        "snapshot_id": snapshot_id,
        "ranges": call_ranges if call_ranges else [],
        "pages": pages_result,
        "attempted_total": attempted_total,
        "inserted_total": inserted_total,
        "duplicates_total": duplicates_total,
        "errors_total": errors_total,
        "status": status,
        "total_count": total_count if "total_count" in locals() else None,
        "decided_page_count": decided_page_count if "decided_page_count" in locals() else None,
    }
