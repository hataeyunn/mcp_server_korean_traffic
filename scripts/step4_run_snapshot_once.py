"""STEP 4 실행 스크립트: Snapshot Runner 단발 실행.

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

from ingestion.pipeline.raw_ingest import generate_snapshot_id  # noqa: E402
from ingestion.providers.seoul_subway import SeoulSubwayArrivalProvider  # noqa: E402
from ingestion.runner.snapshot_runner import run_snapshot_once  # noqa: E402


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


def main() -> None:
    """메인 실행 함수."""
    parser = argparse.ArgumentParser(description="Snapshot Runner 단발 실행")
    parser.add_argument(
        "--ranges",
        type=str,
        default=None,
        help='호출 범위 (예: "0-999,1000-1999,2000-2999")',
    )
    parser.add_argument(
        "--snapshot-id",
        type=str,
        default=None,
        help="스냅샷 ID (미지정 시 생성)",
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
    print("STEP 4: Snapshot Runner 단발 실행")
    print("=" * 80)
    print()

    # snapshot_id 생성 또는 사용자 지정 (CLI에서만 임시 제공)
    snapshot_id = args.snapshot_id or generate_snapshot_id()

    # call_ranges 기본값 (CLI에서만 임시 제공)
    if call_ranges is None:
        call_ranges = [(0, 999), (1000, 1999), (2000, 2999)]

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
    def clock() -> datetime:
        return datetime.now(pytz.timezone("Asia/Seoul"))

    try:
        # Snapshot 실행
        result = run_snapshot_once(
            snapshot_id=snapshot_id,
            call_ranges=call_ranges,
            provider=provider,
            mysql_conn=mysql_conn,
            clock=clock,
        )

        # 결과 출력
        print(f"snapshot_id: {result['snapshot_id']}")
        print(f"status: {result['status']}")
        print()

        # 페이지별 결과 테이블
        print("페이지별 결과:")
        print("-" * 80)
        print(f"{'Start':<10} {'End':<10} {'Status':<10} {'Attempted':<12} {'Inserted':<12} {'Duplicates':<12} {'Error':<30}")
        print("-" * 80)
        for page in result["pages"]:
            error_msg = page["error"][:28] + "..." if page["error"] and len(page["error"]) > 30 else (page["error"] or "")
            print(
                f"{page['start']:<10} {page['end']:<10} {page['status']:<10} "
                f"{page['attempted_rows']:<12} {page['inserted_rows']:<12} "
                f"{page['skipped_duplicates']:<12} {error_msg:<30}"
            )
        print("-" * 80)
        print()

        # 전체 요약
        print("전체 요약:")
        print(f"  attempted_total: {result['attempted_total']}")
        print(f"  inserted_total: {result['inserted_total']}")
        print(f"  duplicates_total: {result['duplicates_total']}")
        print(f"  errors_total: {result['errors_total']}")
        print()

        # DB 검증 쿼리
        print("=" * 80)
        print("DB 검증 (snapshot_id 기준 조회)")
        print("=" * 80)

        cursor = mysql_conn.cursor()

        # COUNT 쿼리
        cursor.execute(
            "SELECT COUNT(*) FROM subway_arrival_raw WHERE snapshot_id = %s",
            (result["snapshot_id"],),
        )
        count = cursor.fetchone()[0]
        print(f"COUNT(*) WHERE snapshot_id = '{result['snapshot_id']}': {count}")

        # MIN/MAX 쿼리
        cursor.execute(
            "SELECT MIN(created_at), MAX(created_at) FROM subway_arrival_raw WHERE snapshot_id = %s",
            (result["snapshot_id"],),
        )
        min_created, max_created = cursor.fetchone()
        print(f"MIN(created_at): {min_created}")
        print(f"MAX(created_at): {max_created}")

        cursor.close()

        print("=" * 80)

        # 종료 코드 결정
        if result["status"] in ("ok", "partial"):
            sys.exit(0)
        else:
            sys.exit(2)

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        # MySQL 연결 종료 (CLI에서만 허용)
        mysql_conn.close()


if __name__ == "__main__":
    main()

