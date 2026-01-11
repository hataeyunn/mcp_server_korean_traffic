"""Scheduler Orchestrator: 시간대 정책 + 예산 가드 + Snapshot Runner 결합."""

from datetime import datetime
from typing import Any

from ingestion.budget import check_budget
from ingestion.runner.snapshot_runner import run_snapshot_once
from ingestion.scheduler import decide_collection


def get_last_snapshot_at(mysql_conn: Any) -> datetime | None:
    """
    가장 최근 snapshot 실행 시각 조회.

    Args:
        mysql_conn: MySQL 연결 객체 (외부 주입)

    Returns:
        가장 최근 snapshot의 created_at 시각 (Asia/Seoul 기준)
        또는 None (snapshot이 없는 경우)

    주의:
        - snapshot 단위 기준으로 조회 (call_log 기준 아님)
        - snapshot_id 또는 created_at 기준으로 가장 최신 스냅샷 시각 계산
    """
    cursor = mysql_conn.cursor()
    try:
        # snapshot_id 기준으로 가장 최근 snapshot의 created_at 조회
        sql = """
        SELECT MAX(created_at) AS last_snapshot_at
        FROM subway_arrival_raw
        """
        cursor.execute(sql)
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
        return None
    finally:
        cursor.close()


def run_orchestrator_once(
    *,
    now: datetime,
    call_ranges: list[tuple[int, int]] | None = None,
    provider,
    mysql_conn,
    clock,
    used_calls_today: int,
) -> dict:
    """
    Orchestrator 단발 실행: 시간대 정책 + 예산 가드 + Snapshot Runner 결합.

    Args:
        now: 현재 시각 (timezone-aware, Asia/Seoul 기준)
        call_ranges: 호출할 범위 리스트 (None이면 Runner에서 동적 결정)
        provider: SeoulSubwayArrivalProvider 인스턴스 (외부 주입)
        mysql_conn: MySQL 연결 객체 (외부 주입)
        clock: 현재 시각을 반환하는 callable (DI)
        used_calls_today: 오늘 사용한 호출 수 (외부 주입, DB/로그/메트릭에서 계산)

    Returns:
        {
            "executed": bool,
            "reason": str,
            "snapshot_id": str | None,
            "last_snapshot_at": datetime | None,
            "interval_seconds": int | None,
            "elapsed_seconds": float | None
        }

    실행 판단 순서:
        1) 시간대 정책 체크 (STEP 5)
        2) 실행 간격 체크 (마지막 snapshot 실행 시각 기준)
        3) 예산 가드 체크 (STEP 6)
        4) 두 조건 모두 통과 시 Snapshot Runner 실행

    주의:
        - 내부에서 datetime.now() 호출 금지
        - provider/mysql_conn/clock 모두 외부 주입
        - Runner 내부 실패(partial/error)는 Orchestrator가 관여하지 않음
    """
    # 1) 시간대 정책 체크 (STEP 5)
    time_policy = decide_collection(now=now)

    if not time_policy["should_collect"]:
        return {
            "executed": False,
            "reason": "time_policy_blocked",
            "snapshot_id": None,
            "last_snapshot_at": None,
            "interval_seconds": None,
            "elapsed_seconds": None,
        }

    interval_seconds = time_policy["interval_seconds"]

    # 2) 실행 간격 체크 (마지막 snapshot 실행 시각 기준)
    last_snapshot_at = get_last_snapshot_at(mysql_conn)

    if last_snapshot_at is not None:
        # last_snapshot_at이 timezone-aware가 아닐 수 있으므로 변환
        if last_snapshot_at.tzinfo is None:
            # MySQL DATETIME은 timezone-naive이므로 Asia/Seoul로 가정
            import pytz

            seoul_tz = pytz.timezone("Asia/Seoul")
            last_snapshot_at = seoul_tz.localize(last_snapshot_at)

        # now와 last_snapshot_at을 같은 timezone으로 맞춤
        if now.tzinfo != last_snapshot_at.tzinfo:
            last_snapshot_at = last_snapshot_at.astimezone(now.tzinfo)

        elapsed_seconds = (now - last_snapshot_at).total_seconds()

        if elapsed_seconds < interval_seconds:
            return {
                "executed": False,
                "reason": "interval_not_elapsed",
                "snapshot_id": None,
                "last_snapshot_at": last_snapshot_at,
                "interval_seconds": interval_seconds,
                "elapsed_seconds": elapsed_seconds,
            }

    # 3) 예산 가드 체크 (STEP 6)
    # [예산 가드 원칙] 항상 최대 가능 호출 수(4회) 기준으로 판단
    # 실제 사용량은 success_pages 기준으로 기록되지만,
    # 예산 판단은 최악의 경우(4회 호출)를 전제로 수행
    budget = check_budget(
        today=now.date(),
        used_calls_today=used_calls_today,
        required_calls=4,  # 항상 최대 4회 기준
    )

    if not budget["should_collect"]:
        return {
            "executed": False,
            "reason": "budget_blocked",
            "snapshot_id": None,
            "last_snapshot_at": last_snapshot_at,
            "interval_seconds": interval_seconds,
            "elapsed_seconds": (
                (now - last_snapshot_at).total_seconds()
                if last_snapshot_at is not None
                else None
            ),
        }

    # 4) snapshot_id 생성 (실행기 책임)
    snapshot_id = now.strftime("%Y%m%d_%H%M%S")

    # 5) Snapshot Runner 호출
    snapshot_result = run_snapshot_once(
        snapshot_id=snapshot_id,
        call_ranges=call_ranges,
        provider=provider,
        mysql_conn=mysql_conn,
        clock=clock,
    )

    # 6) 실행 완료 반환
    elapsed_seconds = (
        (now - last_snapshot_at).total_seconds()
        if last_snapshot_at is not None
        else None
    )

    return {
        "executed": True,
        "reason": "executed",
        "snapshot_id": snapshot_id,
        "last_snapshot_at": last_snapshot_at,
        "interval_seconds": interval_seconds,
        "elapsed_seconds": elapsed_seconds,
        "total_count": snapshot_result.get("total_count"),
        "decided_page_count": snapshot_result.get("decided_page_count"),
    }
