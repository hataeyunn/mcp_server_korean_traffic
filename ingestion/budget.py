"""일일 호출 예산 가드 (순수 판단 로직)."""

from datetime import date


def check_budget(
    *,
    today: date,
    used_calls_today: int,
    required_calls: int,
    daily_limit: int = 1000,
) -> dict:
    """
    예산 체크: 오늘 남은 호출 예산이 요구되는 호출 수를 충족하는지 판단.

    Args:
        today: 오늘 날짜 (Asia/Seoul 기준, 외부에서 주입)
        used_calls_today: 오늘 사용한 호출 수 (외부에서 주입, DB/로그/메트릭에서 계산)
        required_calls: 이번 스냅샷 실행에 필요한 호출 수 (call_ranges 길이)
        daily_limit: 하루 최대 호출 제한 (기본값: 1000)

    Returns:
        {
            "should_collect": bool,
            "remaining_calls": int,
            "reason": str
        }

    판단 규칙:
        - remaining_calls = daily_limit - used_calls_today
        - remaining_calls >= required_calls → should_collect=True, reason="budget_ok"
        - remaining_calls < required_calls → should_collect=False, reason="budget_exceeded"
        - remaining_calls < 0 이면 0으로 간주
        - used_calls_today > daily_limit 인 경우도 should_collect=False

    주의:
        - 이 함수는 순수 판단 함수입니다.
        - DB 접근, 파일 쓰기, datetime.now()/date.today() 호출 금지
        - 날짜 변경 시 리셋은 상위 계층(DB 조회 로직) 책임
    """
    # 남은 호출 수 계산
    remaining_calls = daily_limit - used_calls_today

    # remaining_calls < 0 이면 0으로 간주
    if remaining_calls < 0:
        remaining_calls = 0

    # 예산 초과 판단
    if remaining_calls >= required_calls:
        # 예산 충분: 수집 허용
        return {
            "should_collect": True,
            "remaining_calls": remaining_calls,
            "reason": "budget_ok",
        }
    else:
        # 예산 부족: 수집 차단
        return {
            "should_collect": False,
            "remaining_calls": remaining_calls,
            "reason": "budget_exceeded",
        }
