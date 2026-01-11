"""서울 지하철 실시간 도착정보 OpenAPI Provider Adapter."""

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class PageResult:
    """단일 페이지 호출 결과."""

    start: int
    end: int
    total_count: int | None
    row_count: int
    rows: list[dict[str, str]]


@dataclass
class ProviderResult:
    """Provider 호출 결과."""

    pages: list[PageResult]
    all_rows: list[dict[str, str]]  # 3회 호출 결과를 단순 합친 것
    first_row_keys: set[str] | None


def parse_xml(xml_text: str) -> tuple[str, str, int | None, list[dict[str, str]]]:
    """
    XML 응답을 파싱하여 (result_code, result_message, total_count, rows) 반환.

    Args:
        xml_text: XML 응답 문자열

    Returns:
        (result_code, result_message, total_count, rows) 튜플
        rows는 각 row를 dict[str, str]로 변환한 리스트
        total_count는 None일 수 있음

    Raises:
        RuntimeError: XML 파싱 실패 또는 필수 필드 누락 시
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        raise RuntimeError(f"XML 파싱 실패: {e}") from e

    # RESULT 정보 추출
    result_elem = root.find("RESULT")
    if result_elem is None:
        raise RuntimeError("XML에 RESULT 요소가 없습니다")

    # code는 소문자일 수 있음
    result_code = (
        result_elem.findtext("CODE", "") or result_elem.findtext("code", "")
    ).strip()
    # message도 소문자일 수 있음
    result_message = (
        result_elem.findtext("MESSAGE", "") or result_elem.findtext("message", "")
    ).strip()

    # totalCount 추출: RESULT 내부의 total 또는 루트의 totalCount 또는 row 내부의 totalCount
    total_count: int | None = None
    total_text = result_elem.findtext("total") or root.findtext("totalCount")
    if total_text:
        try:
            total_count = int(total_text.strip())
        except (ValueError, AttributeError):
            pass  # total_count는 None으로 유지
    # row 내부의 totalCount도 확인 (fallback)
    if total_count is None:
        first_row = root.find("row")
        if first_row is not None:
            row_total_text = first_row.findtext("totalCount")
            if row_total_text:
                try:
                    total_count = int(row_total_text.strip())
                except (ValueError, AttributeError):
                    pass

    # rows 추출
    rows = []
    row_elems = root.findall("row")

    for row_elem in row_elems:
        row_dict: dict[str, str] = {}
        _parse_row_element(row_elem, row_dict, "")
        rows.append(row_dict)

    return result_code, result_message, total_count, rows


def _parse_row_element(elem: ET.Element, row_dict: dict[str, str], prefix: str = "") -> None:
    """
    XML 요소를 재귀적으로 파싱하여 dict에 추가.

    중복 태그 처리를 위해 prefix를 사용하여 충돌 회피.
    무손실 파싱을 위해 모든 태그를 key로 변환하고 값은 문자열로 저장.
    무손실의 범위는 XML 태그와 그 text로 한정 (tail은 제외).
    """
    for child in elem:
        key = prefix + child.tag if prefix else child.tag

        # 자식 요소가 있으면 재귀적으로 파싱
        if len(child) > 0:
            # 중첩된 구조인 경우 재귀적으로 파싱
            _parse_row_element(child, row_dict, f"{key}_")
        else:
            # 자식이 없는 경우 (텍스트만 있거나 빈 요소)
            text = child.text.strip() if child.text else ""
            # 같은 key가 이미 존재하면 충돌 회피
            if key in row_dict:
                # 이미 존재하는 경우, 번호를 붙여서 저장
                counter = 1
                new_key = f"{key}__{counter}"
                while new_key in row_dict:
                    counter += 1
                    new_key = f"{key}__{counter}"
                key = new_key
            row_dict[key] = text


class SeoulSubwayArrivalProvider:
    """서울 지하철 실시간 도착정보 OpenAPI Provider."""

    # 기본 호출 범위 (STEP 1용)
    DEFAULT_CALL_RANGES = [(0, 999), (1000, 1999), (2000, 2999)]

    def __init__(
        self,
        api_key: str,
        base_url: str = "http://swopenAPI.seoul.go.kr/api/subway",
        service: str = "realtimeStationArrival",
    ):
        """
        Provider 초기화.

        Args:
            api_key: 서울시 OpenAPI 키
            base_url: API 기본 URL
            service: 서비스명 (기본값: realtimeStationArrival)

        NOTE:
        서울 OpenAPI 명세상 statnNm은 필수이나,
        현재 전체 데이터 수집을 위해 빈 문자열 호출이
        undocumented behavior로 동작하고 있음.
        이 동작은 STEP 1에서만 허용되며,
        이후 단계에서 엔드포인트 재검증 필요.
        """
        self.api_key = api_key
        self.base_url = base_url
        self.service = service
        # 역명은 빈 문자열로 설정하여 전체 수집 (undocumented behavior)
        # NOTE: Runner/Repo/CLI에서 station_name을 알면 안 됨. Provider 내부 private로만 유지
        self._station_name = ""

    def _build_url(self, start: int, end: int) -> str:
        """
        API URL 생성 (내부 메서드).

        Args:
            start: 시작 인덱스 (inclusive)
            end: 종료 인덱스 (inclusive)

        Returns:
            완성된 API URL
        """
        # 역명이 빈 문자열이면 URL 끝에 슬래시만 추가
        if self._station_name:
            return f"{self.base_url}/{self.api_key}/xml/{self.service}/{start}/{end}/{self._station_name}"
        else:
            return f"{self.base_url}/{self.api_key}/xml/{self.service}/{start}/{end}/"

    def fetch_page(self, start: int, end: int) -> dict[str, Any]:
        """
        단일 페이지를 호출하여 데이터를 가져옴.

        Args:
            start: 시작 인덱스 (inclusive)
            end: 종료 인덱스 (inclusive)

        Returns:
            {
                "result_code": str,
                "result_message": str,
                "total_count": int | None,
                "rows": list[dict[str, str]]
            }

        Raises:
            RuntimeError: HTTP 오류 또는 API 오류 시
        """
        url = self._build_url(start, end)

        try:
            response = requests.get(url, timeout=(5, 15))
        except requests.RequestException as e:
            raise RuntimeError(f"HTTP 요청 실패: {e}") from e

        if response.status_code != 200:
            raise RuntimeError(
                f"HTTP 상태 코드 오류: {response.status_code}, URL: {url}"
            )

        result_code, result_message, total_count, rows = parse_xml(response.text)

        # API 결과 코드 검증
        if result_code != "INFO-000":
            raise RuntimeError(
                f"API 오류: CODE={result_code}, MESSAGE={result_message}, URL: {url}"
            )

        return {
            "result_code": result_code,
            "result_message": result_message,
            "total_count": total_count,
            "rows": rows,
        }

    def fetch_pages(self, ranges: list[tuple[int, int]]) -> list[dict[str, Any]]:
        """
        여러 페이지를 순회하며 호출.

        Args:
            ranges: 호출할 범위 리스트

        Returns:
            각 페이지의 fetch_page 결과 리스트
        """
        results = []
        for start, end in ranges:
            result = self.fetch_page(start, end)
            results.append(result)
        return results

    def fetch_fixed_pages(
        self, call_ranges: list[tuple[int, int]] | None = None
    ) -> ProviderResult:
        """
        지정된 범위로 고정 호출을 수행.

        Args:
            call_ranges: 호출할 범위 리스트. None이면 DEFAULT_CALL_RANGES 사용.

        Returns:
            ProviderResult 객체

        Raises:
            RuntimeError: HTTP 오류, RESULT.code != INFO-000, XML 파싱 실패 시
        """
        pages: list[PageResult] = []
        all_rows: list[dict[str, str]] = []

        # 호출 범위 결정 (기본값 사용 또는 인자 사용)
        if call_ranges is None:
            call_ranges = self.DEFAULT_CALL_RANGES

        for start, end in call_ranges:
            page_data = self.fetch_page(start, end)
            total_count = page_data["total_count"]
            rows = page_data["rows"]

            page_result = PageResult(
                start=start,
                end=end,
                total_count=total_count,
                row_count=len(rows),
                rows=rows,
            )
            pages.append(page_result)
            all_rows.extend(rows)

        # 검증: 호출이 실제로 수행되었는지 확인
        assert len(pages) == len(call_ranges), (
            f"호출 횟수가 예상과 다릅니다: 예상={len(call_ranges)}, 실제={len(pages)}"
        )

        # first_row_keys 추출
        first_row_keys: set[str] | None = None
        if all_rows:
            first_row_keys = set(all_rows[0].keys())

        return ProviderResult(
            pages=pages,
            all_rows=all_rows,
            first_row_keys=first_row_keys,
        )
