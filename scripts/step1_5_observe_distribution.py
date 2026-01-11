"""STEP 1.5 (B): 데이터 분포 관측 & 엔드포인트 검증 (판단 금지 단계).

필수 패키지 설치:
    pip install requests python-dotenv

또는 requirements.txt 사용:
    pip install -r requirements.txt
"""

import os
import sys
from collections import Counter
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
    print("=" * 80)
    print("서울 지하철 실시간 도착정보 API - 데이터 분포 관측 리포트")
    print("=" * 80)
    print()

    try:
        call_ranges = [(0, 999), (1000, 1999), (2000, 2999)]
        result = provider.fetch_fixed_pages(call_ranges=call_ranges)
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except AssertionError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    # [1] 호출 요약
    print("[1] 호출 요약")
    print("-" * 80)
    print(f"호출 횟수: {len(result.pages)}")
    print()
    for idx, page in enumerate(result.pages, 1):
        print(f"호출 {idx}:")
        print(f"  start: {page.start}")
        print(f"  end: {page.end}")
        print(f"  row_count: {page.row_count}")
        print(f"  reported totalCount: {page.total_count if page.total_count is not None else 'N/A'}")
        print()
    print()

    # [2] 전체 row 통계
    print("[2] 전체 row 통계")
    print("-" * 80)
    all_rows = result.all_rows
    print(f"전체 row 수 (3회 호출 결과 합): {len(all_rows)}")
    print()

    # rowNum 값 분포
    row_nums = []
    for row in all_rows:
        row_num_str = row.get("rowNum", "")
        if row_num_str:
            try:
                row_nums.append(int(row_num_str))
            except ValueError:
                pass

    if row_nums:
        print("rowNum 값 분포:")
        print(f"  min: {min(row_nums)}")
        print(f"  max: {max(row_nums)}")
        print()

        # rowNum 중복 여부
        row_num_counter = Counter(row_nums)
        duplicates = {num: count for num, count in row_num_counter.items() if count > 1}
        if duplicates:
            print("rowNum 중복 발견:")
            for row_num, count in sorted(duplicates.items()):
                print(f"  rowNum={row_num}: {count}회")
        else:
            print("rowNum 중복: 없음")
    else:
        print("rowNum 값: 없음 또는 파싱 불가")
    print()
    print()

    # [3] 역(statnNm) 분포
    print("[3] 역(statnNm) 분포")
    print("-" * 80)
    statn_nms = [row.get("statnNm", "") for row in all_rows if row.get("statnNm")]
    statn_nm_counter = Counter(statn_nms)
    unique_statn_nms = len(statn_nm_counter)
    print(f"고유 statnNm 개수: {unique_statn_nms}")
    print()

    if unique_statn_nms == 1:
        print("statnNm 분포: 단일 값")
        print(f"  값: {list(statn_nm_counter.keys())[0]}")
        print(f"  빈도: {list(statn_nm_counter.values())[0]}")
    else:
        print("statnNm 분포: 다수 값")
        print("statnNm 상위 20개 (빈도수 포함):")
        for statn_nm, count in statn_nm_counter.most_common(20):
            print(f"  {statn_nm}: {count}회")
    print()
    print()

    # [4] 노선(subwayId) 분포
    print("[4] 노선(subwayId) 분포")
    print("-" * 80)
    subway_ids = [row.get("subwayId", "") for row in all_rows if row.get("subwayId")]
    subway_id_counter = Counter(subway_ids)
    unique_subway_ids = sorted(set(subway_ids))
    print(f"고유 subwayId 목록: {unique_subway_ids}")
    print()
    print("subwayId별 row 개수:")
    for subway_id in unique_subway_ids:
        count = subway_id_counter[subway_id]
        print(f"  {subway_id}: {count}개")
    print()
    print()

    # [5] 필드 구조 안정성
    print("[5] 필드 구조 안정성")
    print("-" * 80)
    if not all_rows:
        print("row가 없어서 필드 구조를 확인할 수 없습니다.")
    else:
        first_row_keys = set(all_rows[0].keys())
        print(f"첫 row의 key 개수: {len(first_row_keys)}")
        print(f"첫 row의 key 목록: {sorted(first_row_keys)}")
        print()

        # key 개수가 다른 row 찾기
        inconsistent_rows = []
        for idx, row in enumerate(all_rows):
            row_keys = set(row.keys())
            if row_keys != first_row_keys:
                missing_keys = first_row_keys - row_keys
                extra_keys = row_keys - first_row_keys
                row_num = row.get("rowNum", f"인덱스{idx}")
                inconsistent_rows.append({
                    "row_num": row_num,
                    "index": idx,
                    "missing_keys": missing_keys,
                    "extra_keys": extra_keys,
                })

        if inconsistent_rows:
            print(f"key 개수가 다른 row 발견: {len(inconsistent_rows)}개")
            print("상세 정보:")
            for item in inconsistent_rows[:10]:  # 최대 10개만 출력
                print(f"  rowNum={item['row_num']} (인덱스={item['index']}):")
                if item["missing_keys"]:
                    print(f"    누락된 key: {sorted(item['missing_keys'])}")
                if item["extra_keys"]:
                    print(f"    추가된 key: {sorted(item['extra_keys'])}")
            if len(inconsistent_rows) > 10:
                print(f"  ... 외 {len(inconsistent_rows) - 10}개 더 있음")
        else:
            print("모든 row가 동일한 key 집합을 가집니다.")
    print()
    print()

    # [6] 샘플 출력
    print("[6] 샘플 출력")
    print("-" * 80)
    print("rowNum 기준 상위 5개 row:")
    print()

    # rowNum으로 정렬 가능한 row 찾기
    sortable_rows = []
    for idx, row in enumerate(all_rows):
        row_num_str = row.get("rowNum", "")
        try:
            row_num = int(row_num_str) if row_num_str else idx
            sortable_rows.append((row_num, idx, row))
        except ValueError:
            sortable_rows.append((idx, idx, row))

    # rowNum 기준으로 정렬
    sortable_rows.sort(key=lambda x: x[0])

    for rank, (row_num, orig_idx, row) in enumerate(sortable_rows[:5], 1):
        print(f"[{rank}] rowNum={row.get('rowNum', 'N/A')}")
        print(f"    statnNm: {row.get('statnNm', 'N/A')}")
        print(f"    subwayId: {row.get('subwayId', 'N/A')}")
        print(f"    trainLineNm: {row.get('trainLineNm', 'N/A')}")
        print(f"    recptnDt: {row.get('recptnDt', 'N/A')}")
        print()

    print("=" * 80)
    print("관측 리포트 완료")
    print("=" * 80)


if __name__ == "__main__":
    main()

