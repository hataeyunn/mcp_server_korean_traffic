"""STEP 1 테스트: XML 파싱 무손실 검증 (네트워크 없이)."""

import sys
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from ingestion.providers.seoul_subway import parse_xml  # noqa: E402

# 서울 지하철 API XML 샘플 (0~5 범위 조회 예시)
XML_SAMPLE = """<?xml version="1.0" encoding="UTF-8"?>
<realtimeStationArrival>
    <RESULT>
        <CODE>INFO-000</CODE>
        <MESSAGE>정상 처리되었습니다.</MESSAGE>
    </RESULT>
    <totalCount>19</totalCount>
    <row>
        <subwayId>1001</subwayId>
        <updnLine>상행</updnLine>
        <trainLineNm>1호선</trainLineNm>
        <statnNm>서울역</statnNm>
        <recptnDt>20240101120000</recptnDt>
        <arvlCd>0</arvlCd>
        <arvlMsg2>도착</arvlMsg2>
        <arvlMsg3>서울역 도착</arvlMsg3>
        <lstcarAt>1</lstcarAt>
        <rowNum>0</rowNum>
    </row>
    <row>
        <subwayId>1002</subwayId>
        <updnLine>하행</updnLine>
        <trainLineNm>1호선</trainLineNm>
        <statnNm>서울역</statnNm>
        <recptnDt>20240101120010</recptnDt>
        <arvlCd>1</arvlCd>
        <arvlMsg2>출발</arvlMsg2>
        <arvlMsg3>서울역 출발</arvlMsg3>
        <lstcarAt>2</lstcarAt>
        <rowNum>1</rowNum>
    </row>
    <row>
        <subwayId>1003</subwayId>
        <updnLine>상행</updnLine>
        <trainLineNm>2호선</trainLineNm>
        <statnNm>강남역</statnNm>
        <recptnDt>20240101120020</recptnDt>
        <arvlCd>2</arvlCd>
        <arvlMsg2>진입</arvlMsg2>
        <arvlMsg3>강남역 진입</arvlMsg3>
        <lstcarAt>3</lstcarAt>
        <rowNum>2</rowNum>
    </row>
    <row>
        <subwayId>1004</subwayId>
        <updnLine>하행</updnLine>
        <trainLineNm>2호선</trainLineNm>
        <statnNm>강남역</statnNm>
        <recptnDt>20240101120030</recptnDt>
        <arvlCd>3</arvlCd>
        <arvlMsg2>전역출발</arvlMsg2>
        <arvlMsg3>강남역 전역출발</arvlMsg3>
        <lstcarAt>4</lstcarAt>
        <rowNum>3</rowNum>
    </row>
    <row>
        <subwayId>1005</subwayId>
        <updnLine>상행</updnLine>
        <trainLineNm>3호선</trainLineNm>
        <statnNm>종로3가역</statnNm>
        <recptnDt>20240101120040</recptnDt>
        <arvlCd>4</arvlCd>
        <arvlMsg2>전역진입</arvlMsg2>
        <arvlMsg3>종로3가역 전역진입</arvlMsg3>
        <lstcarAt>5</lstcarAt>
        <rowNum>4</rowNum>
    </row>
</realtimeStationArrival>
"""


def test_parse_xml_result_code() -> None:
    """RESULT.code가 INFO-000인지 검증."""
    result_code, _, _, _ = parse_xml(XML_SAMPLE)
    assert result_code == "INFO-000", f"예상: INFO-000, 실제: {result_code}"


def test_parse_xml_total_count() -> None:
    """total_count가 19인지 검증."""
    _, _, total_count, _ = parse_xml(XML_SAMPLE)
    assert total_count == 19, f"예상: 19, 실제: {total_count}"


def test_parse_xml_rows_count() -> None:
    """rows 개수가 5인지 검증 (샘플은 0~5로 조회된 예)."""
    _, _, _, rows = parse_xml(XML_SAMPLE)
    assert len(rows) == 5, f"예상: 5, 실제: {len(rows)}"


def test_parse_xml_first_row_fields() -> None:
    """첫 번째 row에 모든 필수 필드가 존재하는지 검증."""
    _, _, _, rows = parse_xml(XML_SAMPLE)
    assert len(rows) > 0, "rows가 비어있습니다"

    first_row = rows[0]
    required_fields = [
        "subwayId",
        "updnLine",
        "trainLineNm",
        "statnNm",
        "recptnDt",
        "arvlCd",
        "arvlMsg2",
        "arvlMsg3",
        "lstcarAt",
        "rowNum",
    ]

    for field in required_fields:
        assert field in first_row, f"필수 필드 '{field}'가 누락되었습니다"
        assert isinstance(first_row[field], str), f"필드 '{field}'의 타입이 str이 아닙니다: {type(first_row[field])}"


def test_parse_xml_field_values() -> None:
    """필드 값이 올바르게 파싱되었는지 검증."""
    _, _, _, rows = parse_xml(XML_SAMPLE)
    assert len(rows) > 0, "rows가 비어있습니다"

    first_row = rows[0]
    assert first_row["subwayId"] == "1001"
    assert first_row["updnLine"] == "상행"
    assert first_row["trainLineNm"] == "1호선"
    assert first_row["statnNm"] == "서울역"
    assert first_row["rowNum"] == "0"


def test_parse_xml_all_rows_have_same_keys() -> None:
    """모든 row가 동일한 키 집합을 가지는지 검증 (무손실)."""
    _, _, _, rows = parse_xml(XML_SAMPLE)
    assert len(rows) > 0, "rows가 비어있습니다"

    baseline_keys = set(rows[0].keys())

    for idx, row in enumerate(rows):
        row_keys = set(row.keys())
        assert row_keys == baseline_keys, (
            f"row[{idx}]의 키가 baseline과 다릅니다. "
            f"baseline: {sorted(baseline_keys)}, "
            f"row[{idx}]: {sorted(row_keys)}"
        )


def test_parse_xml_all_values_are_strings() -> None:
    """모든 값이 문자열 타입인지 검증 (무손실)."""
    _, _, _, rows = parse_xml(XML_SAMPLE)

    for idx, row in enumerate(rows):
        for key, value in row.items():
            assert isinstance(value, str), (
                f"row[{idx}].{key}의 타입이 str이 아닙니다: {type(value)}"
            )

