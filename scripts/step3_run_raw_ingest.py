"""STEP 3 실행 스크립트: Raw 데이터 적재.

필수 패키지 설치:
    pip install pymysql python-dotenv pytz

또는 requirements.txt 사용:
    pip install -r requirements.txt
"""

import os
import sys
from pathlib import Path

import pymysql
from dotenv import load_dotenv  # noqa: E402

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.pipeline.raw_ingest import (  # noqa: E402
    generate_snapshot_id,
    ingest_provider_result,
)
from ingestion.providers.seoul_subway import SeoulSubwayArrivalProvider  # noqa: E402


def main() -> None:
    """메인 실행 함수."""
    # 최상위 .env 로드
    env_path = project_root / ".env"
    load_dotenv(env_path)

    # API Key 확인
    api_key = os.getenv("SEOUL_SUBWAY_API_KEY")
    if not api_key:
        print("ERROR: SEOUL_SUBWAY_API_KEY가 .env에 없습니다.", file=sys.stderr)
        sys.exit(1)

    # MySQL 접속 정보
    mysql_host = os.getenv("MYSQL_HOST", "localhost")
    mysql_port = int(os.getenv("MYSQL_PORT", "3306"))
    mysql_user = os.getenv("MYSQL_USER", "root")
    mysql_password = os.getenv("MYSQL_PASSWORD", "")
    mysql_database = os.getenv("MYSQL_DATABASE", "mcp_subway")

    print("=" * 80)
    print("STEP 3: Raw 데이터 적재 파이프라인")
    print("=" * 80)
    print()

    # MySQL 연결
    try:
        mysql_conn = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
            charset="utf8mb4",
            autocommit=False,  # 명시적 commit 사용
        )
        print("✓ MySQL 연결 성공")
    except Exception as e:
        print(f"ERROR: MySQL 연결 실패: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        # Provider 실행
        print("서울 지하철 실시간 도착정보 API 호출 중...")
        provider = SeoulSubwayArrivalProvider(api_key=api_key)
        call_ranges = [(0, 999), (1000, 1999), (2000, 2999)]
        provider_result = provider.fetch_fixed_pages(call_ranges=call_ranges)
        print(f"✓ API 호출 완료: {len(provider_result.all_rows)}개 row 수집")
        print()

        # snapshot_id 생성
        snapshot_id = generate_snapshot_id()
        print(f"snapshot_id: {snapshot_id}")
        print()

        # Raw 데이터 적재
        print("Raw 테이블에 적재 중...")
        result = ingest_provider_result(provider_result, snapshot_id, mysql_conn)
        print("✓ 적재 완료")
        print()

        # 결과 요약 출력
        print("=" * 80)
        print("적재 결과 요약")
        print("=" * 80)
        print(f"snapshot_id: {snapshot_id}")
        print(f"attempted_rows: {result['attempted_rows']}")
        print(f"inserted_rows: {result['inserted_rows']}")
        print(f"skipped_duplicates: {result['skipped_duplicates']}")
        print("=" * 80)

    except Exception as e:
        mysql_conn.rollback()
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        mysql_conn.close()


if __name__ == "__main__":
    main()

