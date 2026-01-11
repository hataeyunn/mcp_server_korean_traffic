"""STEP 3 테스트: payload_hash 계산 검증."""

import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.pipeline.raw_ingest import compute_payload_hash  # noqa: E402


def test_payload_hash_same_for_different_key_order() -> None:
    """같은 dict라도 key 순서가 다르면 hash가 같아야 함."""
    payload1 = {
        "subwayId": "1001",
        "statnNm": "서울역",
        "trainLineNm": "1호선",
        "rowNum": "1",
    }

    payload2 = {
        "rowNum": "1",
        "trainLineNm": "1호선",
        "statnNm": "서울역",
        "subwayId": "1001",
    }

    hash1 = compute_payload_hash(payload1)
    hash2 = compute_payload_hash(payload2)

    assert hash1 == hash2, (
        f"key 순서가 다른데 hash가 다릅니다: "
        f"hash1={hash1}, hash2={hash2}"
    )


def test_payload_hash_length() -> None:
    """hash 길이가 64자여야 함."""
    payload = {"subwayId": "1001", "statnNm": "서울역"}
    hash_value = compute_payload_hash(payload)

    assert len(hash_value) == 64, f"hash 길이가 64가 아닙니다: {len(hash_value)}"


def test_payload_hash_type() -> None:
    """hash가 문자열 타입이어야 함."""
    payload = {"subwayId": "1001", "statnNm": "서울역"}
    hash_value = compute_payload_hash(payload)

    assert isinstance(hash_value, str), f"hash 타입이 str이 아닙니다: {type(hash_value)}"


def test_payload_hash_different_for_different_content() -> None:
    """내용이 다르면 hash도 달라야 함."""
    payload1 = {"subwayId": "1001", "statnNm": "서울역"}
    payload2 = {"subwayId": "1002", "statnNm": "서울역"}

    hash1 = compute_payload_hash(payload1)
    hash2 = compute_payload_hash(payload2)

    assert hash1 != hash2, "내용이 다른데 hash가 같습니다"


def test_payload_hash_with_unicode() -> None:
    """유니코드 문자가 포함되어도 정상 작동해야 함."""
    payload = {
        "subwayId": "1001",
        "statnNm": "서울역",
        "trainLineNm": "1호선",
        "arvlMsg2": "도착",
    }

    hash_value = compute_payload_hash(payload)

    assert len(hash_value) == 64
    assert isinstance(hash_value, str)


def test_payload_hash_empty_dict() -> None:
    """빈 dict도 처리 가능해야 함."""
    payload = {}
    hash_value = compute_payload_hash(payload)

    assert len(hash_value) == 64
    assert isinstance(hash_value, str)

