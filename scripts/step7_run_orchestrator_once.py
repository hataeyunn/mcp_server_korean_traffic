"""STEP 7 실행 스크립트: Orchestrator 단발 실행.

필수 패키지 설치:
    pip install pymysql python-dotenv pytz requests

또는 requirements.txt 사용:
    pip install -r requirements.txt
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import pymysql
import pytz
from dotenv import load_dotenv  # noqa: E402

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.orchestrator import run_orchestrator_once  # noqa: E402
from ingestion.providers.seoul_subway import SeoulSubwayArrivalProvider  # noqa: E402


def parse_ranges(ranges_str: str) -> list[tuple[int, int]]:
    """
    범위 문자열을 파싱.

    예: "0-999,1000-1999,2000-2999" -> [(0, 999), (1000, 1999), (2000, 2999)]
    """
    ranges = []
    for range_str in ranges_str.split(","):
        range_str = range_str.strip()
        if "-" in range_str:
            start_str, end_str = range_str.split("-", 1)
            ranges.append((int(start_str.strip()), int(end_str.strip())))
        else:
            raise ValueError(f"잘못된 범위 형식: {range_str}")
    return ranges


def get_used_calls_today(mysql_conn, today: datetime) -> int:
    """
    오늘 사용한 호출 수 조회.

    Args:
        mysql_conn: MySQL 연결 객체
        today: 오늘 날짜 (Asia/Seoul 기준)

    Returns:
        오늘 사용한 호출 수 (success 상태만 카운트)

    STEP 8.5 DONE:
    Budget 계산은 subway_api_call_log를
    API 호출 횟수의 단일 진실 소스로 사용한다.
    """
    # 날짜를 YYYY-MM-DD 형식으로 변환
    date_str = today.strftime("%Y-%m-%d")

    cursor = mysql_conn.cursor()
    try:
        # Call Log 테이블 기준으로 오늘 날짜의 success 호출 수 계산
        sql = """
        SELECT COUNT(*) AS call_count
        FROM subway_api_call_log
        WHERE call_date = %s
          AND status = 'success'
        """
        cursor.execute(sql, (date_str,))
        result = cursor.fetchone()
        return result[0] if result else 0
    finally:
        cursor.close()


def main() -> None:
    """메인 실행 함수."""
    parser = argparse.ArgumentParser(description="Orchestrator 단발 실행")
    parser.add_argument(
        "--ranges",
        type=str,
        default=None,
        help='호출 범위 (예: "0-999,1000-1999,2000-2999")',
    )
    args = parser.parse_args()

    # .env 로드
    env_path = project_root / ".env"
    load_dotenv(env_path)

    # 범위 파싱
    call_ranges = None
    if args.ranges:
        try:
            call_ranges = parse_ranges(args.ranges)
        except ValueError as e:
            print(f"ERROR: 범위 파싱 실패: {e}", file=sys.stderr)
            sys.exit(1)

    print("=" * 80)
    print("STEP 7: Orchestrator 단발 실행")
    print("=" * 80)
    print()

    # call_ranges 기본값: None이면 Runner에서 동적으로 결정 (totalCount 기반)
    # call_ranges를 명시적으로 전달하면 그대로 사용 (기존 동작 유지)

    # Provider 생성 (CLI에서만 허용)
    api_key = os.getenv("SEOUL_SUBWAY_API_KEY")
    if not api_key:
        print("ERROR: SEOUL_SUBWAY_API_KEY가 .env에 없습니다.", file=sys.stderr)
        sys.exit(1)
    provider = SeoulSubwayArrivalProvider(api_key=api_key)

    # MySQL 연결 생성 (CLI에서만 허용)
    mysql_host = os.getenv("MYSQL_HOST", "localhost")
    mysql_port = int(os.getenv("MYSQL_PORT", "3306"))
    mysql_user = os.getenv("MYSQL_USER", "root")
    mysql_password = os.getenv("MYSQL_PASSWORD", "")
    mysql_database = os.getenv("MYSQL_DATABASE", "mcp_subway")

    mysql_conn = pymysql.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database,
        charset="utf8mb4",
        autocommit=False,
    )

    # clock 함수 정의 (DI)
    seoul_tz = pytz.timezone("Asia/Seoul")

    def clock() -> datetime:
        return datetime.now(seoul_tz)

    try:
        # 현재 시각
        now = clock()

        # 오늘 사용한 호출 수 조회
        used_calls_today = get_used_calls_today(mysql_conn, now)

        print(f"현재 시각: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        print(f"오늘 사용한 호출 수: {used_calls_today}")
        if call_ranges:
            print(f"요구되는 호출 수: {len(call_ranges)} (고정)")
        else:
            print(f"요구되는 호출 수: 최대 4회 (동적 결정, totalCount 기반)")
        print()

        # Orchestrator 실행
        result = run_orchestrator_once(
            now=now,
            call_ranges=call_ranges,
            provider=provider,
            mysql_conn=mysql_conn,
            clock=clock,
            used_calls_today=used_calls_today,
        )

        # 결과 출력
        print("=" * 80)
        print("실행 결과")
        print("=" * 80)
        print(f"executed: {result['executed']}")
        print(f"reason: {result['reason']}")
        if result["snapshot_id"]:
            print(f"snapshot_id: {result['snapshot_id']}")
        if result.get("last_snapshot_at"):
            print(f"last_snapshot_at: {result['last_snapshot_at']}")
        if result.get("interval_seconds") is not None:
            print(f"interval_seconds: {result['interval_seconds']}")
        if result.get("elapsed_seconds") is not None:
            print(f"elapsed_seconds: {result['elapsed_seconds']:.1f}")
        # 실행 후 totalCount 정보 표시
        if result.get("executed") and result.get("total_count") is not None:
            total_count = result["total_count"]
            decided_page_count = result.get("decided_page_count")
            if decided_page_count:
                print(f"totalCount: {total_count} (decided_page_count: {decided_page_count})")
            else:
                print(f"totalCount: {total_count}")
        print()

        # 실행된 경우 snapshot row count 요약
        if result["executed"] and result["snapshot_id"]:
            cursor = mysql_conn.cursor()
            try:
                cursor.execute(
                    "SELECT COUNT(*) FROM subway_arrival_raw WHERE snapshot_id = %s",
                    (result["snapshot_id"],),
                )
                count = cursor.fetchone()[0]
                print(f"snapshot_id '{result['snapshot_id']}' 기준 row count: {count}")
            finally:
                cursor.close()

        # 종료 코드 결정
        if result["executed"]:
            sys.exit(0)
        else:
            sys.exit(1)

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        # MySQL 연결 종료 (CLI에서만 허용)
        mysql_conn.close()


# 이 파일은 Ingestion Plane 내부 실행 유닛입니다.
# 공식 entrypoint는 main_ingestion.py를 사용하세요.
