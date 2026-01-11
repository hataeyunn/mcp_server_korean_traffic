"""시간대 기반 수집 스케줄러 (순수 로직)."""

from datetime import datetime, timedelta

import pytz

# 시간대별 수집 주기 정책 (권장 수집 간격)
# 주의: interval_seconds는 권장 수집 간격이며, sleep이나 loop가 아닙니다.
# 실제 실행 주기는 상위 계층(스케줄러/타이머)에서 결정합니다.
COMMUTE_INTERVAL_SECONDS = 120  # 출근/퇴근 시간대: 2분
NORMAL_INTERVAL_SECONDS = 900  # 일반 시간대: 15분


def decide_collection(
    *,
    now: datetime,
) -> dict:
    """
    현재 시각 기준으로 수집 여부와 다음 실행 간격을 결정.

    Args:
        now: 현재 시각 (timezone-aware, Asia/Seoul 기준)

    Returns:
        {
            "should_collect": bool,
            "interval_seconds": int,
            "time_bucket": "morning" | "evening" | "normal" | "night"
        }

    시간대 정의:
        - 출근: 07:00:00 ~ 09:30:00
        - 퇴근: 17:30:00 ~ 20:00:00
        - 심야: 00:30:00 ~ 05:00:00 (수집 안 함)
        - 일반: 나머지

    수집 정책:
        - 출근/퇴근: should_collect=True, interval_seconds=120 (2분)
        - 일반: should_collect=True, interval_seconds=900 (15분)
        - 심야: should_collect=False, interval_seconds=심야 종료까지 남은 초
    """
    # Asia/Seoul timezone으로 변환
    seoul_tz = pytz.timezone("Asia/Seoul")
    if now.tzinfo is None:
        raise ValueError("now는 timezone-aware datetime이어야 합니다")
    now_seoul = now.astimezone(seoul_tz)

    # 시/분/초 추출
    hour = now_seoul.hour
    minute = now_seoul.minute
    second = now_seoul.second
    time_seconds = hour * 3600 + minute * 60 + second

    # 시간대 판단
    # 출근: 07:00:00 ~ 09:30:00
    morning_start = 7 * 3600  # 07:00:00
    morning_end = 9 * 3600 + 30 * 60  # 09:30:00

    # 퇴근: 17:30:00 ~ 20:00:00
    evening_start = 17 * 3600 + 30 * 60  # 17:30:00
    evening_end = 20 * 3600  # 20:00:00

    # 심야: 00:30:00 ~ 05:00:00
    night_start = 30 * 60  # 00:30:00
    night_end = 5 * 3600  # 05:00:00

    # 심야 판단 (날짜 경계 주의)
    # 00:30:00 ~ 05:00:00 사이가 심야
    if night_start <= time_seconds <= night_end:
        time_bucket = "night"
        should_collect = False
        # 다음 수집 가능 시각은 05:00:01
        next_collect_time = now_seoul.replace(
            hour=5, minute=0, second=1, microsecond=0
        )
        if now_seoul >= next_collect_time:
            # 이미 05:00:01 이후라면 다음날 05:00:01
            next_collect_time = next_collect_time + timedelta(days=1)
        interval_seconds = int((next_collect_time - now_seoul).total_seconds())
        return {
            "should_collect": should_collect,
            "interval_seconds": interval_seconds,
            "time_bucket": time_bucket,
        }

    # 출근 시간대 판단
    if morning_start <= time_seconds <= morning_end:
        time_bucket = "morning"
        should_collect = True
        interval_seconds = COMMUTE_INTERVAL_SECONDS
        return {
            "should_collect": should_collect,
            "interval_seconds": interval_seconds,
            "time_bucket": time_bucket,
        }

    # 퇴근 시간대 판단
    if evening_start <= time_seconds <= evening_end:
        time_bucket = "evening"
        should_collect = True
        interval_seconds = COMMUTE_INTERVAL_SECONDS
        return {
            "should_collect": should_collect,
            "interval_seconds": interval_seconds,
            "time_bucket": time_bucket,
        }

    # 일반 시간대
    time_bucket = "normal"
    should_collect = True
    interval_seconds = NORMAL_INTERVAL_SECONDS
    return {
        "should_collect": should_collect,
        "interval_seconds": interval_seconds,
        "time_bucket": time_bucket,
    }
