"""STEP 1 스모크 테스트: 고정 3회 호출 검증.

필수 패키지 설치:
    pip install requests python-dotenv

또는 requirements.txt 사용:
    pip install -r requirements.txt
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv  # noqa: E402

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.providers.seoul_subway import SeoulSubwayArrivalProvider  # noqa: E402


def main() -> None:
    """메인 실행 함수."""
    # 최상위 .env 로드
    env_path = project_root / ".env"
    load_dotenv(env_path)

    api_key = os.getenv("SEOUL_SUBWAY_API_KEY")
    if not api_key:
        print("ERROR: SEOUL_SUBWAY_API_KEY가 .env에 없습니다.", file=sys.stderr)
        sys.exit(1)

    # Provider 생성 및 호출
    provider = SeoulSubwayArrivalProvider(api_key=api_key)
    print("서울 지하철 실시간 도착정보 API 호출 중...")
    print(f"API Key: {api_key[:10]}...")
    print()

    try:
        result = provider.fetch_fixed_pages()
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except AssertionError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    # 결과 요약 출력
    print("=" * 60)
    print("호출 결과 요약")
    print("=" * 60)
    print(f"호출 횟수: {len(result.pages)}")
    print()

    for idx, page in enumerate(result.pages, 1):
        print(f"Page {idx}:")
        print(f"  start: {page.start}")
        print(f"  end: {page.end}")
        print(f"  row_count: {page.row_count}")
        print(f"  total_count: {page.total_count}")
        print()

    print(f"전체 all_rows 길이: {len(result.all_rows)}")
    print()

    if result.first_row_keys:
        sorted_keys = sorted(result.first_row_keys)
        print(f"first_row_keys ({len(sorted_keys)}개):")
        for key in sorted_keys:
            print(f"  - {key}")
    else:
        print("first_row_keys: None (rows가 비어있음)")
    print()

    print("=" * 60)
    print("검증 완료: 모든 조건을 통과했습니다.")
    print("=" * 60)


if __name__ == "__main__":
    main()
