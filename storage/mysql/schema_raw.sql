-- ============================================================================
-- STEP 2: 서울 지하철 실시간 도착정보 Raw 테이블 스키마
-- ============================================================================
-- 목적: OpenAPI XML <row>를 무손실로 보존하는 append-only 테이블
-- 원칙: 무손실, append-only, 중복 방지, 감사/추적 가능
-- ============================================================================

DROP TABLE IF EXISTS subway_arrival_raw;

CREATE TABLE subway_arrival_raw (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '자동 증가 기본 키',
    
    snapshot_id VARCHAR(64) NOT NULL COMMENT '한 번의 수집 실행(run)을 식별하는 ID',
    
    collected_at DATETIME NOT NULL COMMENT '이 row가 수집된 시각 (UTC or Asia/Seoul)',
    
    page_start INT NOT NULL COMMENT 'OpenAPI 호출 시 사용한 START 범위',
    page_end INT NOT NULL COMMENT 'OpenAPI 호출 시 사용한 END 범위',
    
    raw_payload JSON NOT NULL COMMENT 'XML <row>를 파싱한 dict 전체 (필드 무손실 보존)',
    
    payload_hash CHAR(64) NOT NULL COMMENT 'raw_payload를 canonical JSON으로 만든 뒤 SHA-256 해시 (중복 방지용)',
    
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '레코드 생성 시각'
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='서울 지하철 실시간 도착정보 Raw 데이터 (append-only, 무손실 보존)';

-- ============================================================================
-- 인덱스 및 제약 조건
-- ============================================================================

-- 중복 방지: 동일 raw row 중복 INSERT 방지
ALTER TABLE subway_arrival_raw
    ADD UNIQUE KEY uq_payload_hash (payload_hash);

-- 수집 실행 단위 조회용
ALTER TABLE subway_arrival_raw
    ADD KEY idx_snapshot (snapshot_id);

-- 시계열 조회용
ALTER TABLE subway_arrival_raw
    ADD KEY idx_collected_at (collected_at);

-- 페이지 범위 조회용
ALTER TABLE subway_arrival_raw
    ADD KEY idx_page_range (page_start, page_end);

-- ============================================================================
-- 검증 쿼리
-- ============================================================================

-- 테이블 존재 확인
-- SHOW TABLES LIKE 'subway_arrival_raw';

-- 테이블 구조 확인
-- DESCRIBE subway_arrival_raw;

-- JSON 필드 추출 예시
-- SELECT 
--     id,
--     JSON_EXTRACT(raw_payload, '$.subwayId') AS subway_id,
--     JSON_EXTRACT(raw_payload, '$.statnNm') AS station_name,
--     JSON_EXTRACT(raw_payload, '$.trainLineNm') AS train_line_name,
--     collected_at
-- FROM subway_arrival_raw
-- LIMIT 10;

-- UNIQUE 제약 확인용 예시 INSERT (주석 처리)
-- 주의: 실제 실행 시 snapshot_id, collected_at, page_start, page_end, raw_payload, payload_hash를 실제 값으로 대체해야 함
/*
INSERT INTO subway_arrival_raw (
    snapshot_id,
    collected_at,
    page_start,
    page_end,
    raw_payload,
    payload_hash
) VALUES (
    'snapshot_20260104_215000',
    '2026-01-04 21:50:00',
    0,
    999,
    '{"subwayId": "1001", "statnNm": "서울역", "trainLineNm": "1호선", "rowNum": "1"}',
    SHA2('{"subwayId": "1001", "statnNm": "서울역", "trainLineNm": "1호선", "rowNum": "1"}', 256)
);

-- 동일 payload_hash로 재삽입 시도 (UNIQUE 제약으로 실패해야 함)
INSERT INTO subway_arrival_raw (
    snapshot_id,
    collected_at,
    page_start,
    page_end,
    raw_payload,
    payload_hash
) VALUES (
    'snapshot_20260104_215100',
    '2026-01-04 21:51:00',
    0,
    999,
    '{"subwayId": "1001", "statnNm": "서울역", "trainLineNm": "1호선", "rowNum": "1"}',
    SHA2('{"subwayId": "1001", "statnNm": "서울역", "trainLineNm": "1호선", "rowNum": "1"}', 256)
);
-- 예상 결과: ERROR 1062 (23000): Duplicate entry '...' for key 'uq_payload_hash'
*/

-- 인덱스 확인
-- SHOW INDEX FROM subway_arrival_raw;

-- 테이블 통계
-- SELECT 
--     COUNT(*) AS total_rows,
--     COUNT(DISTINCT snapshot_id) AS unique_snapshots,
--     MIN(collected_at) AS earliest_collection,
--     MAX(collected_at) AS latest_collection
-- FROM subway_arrival_raw;

