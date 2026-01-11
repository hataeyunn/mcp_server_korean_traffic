-- Call Log 테이블: API 호출 기록 (단일 진실 소스)
-- Budget Guard의 used_calls_today 계산 기준

DROP TABLE IF EXISTS subway_api_call_log;

CREATE TABLE subway_api_call_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    call_date DATE NOT NULL COMMENT 'Asia/Seoul 기준 호출 날짜 (Budget 계산용)',
    snapshot_id VARCHAR(64) NOT NULL COMMENT '어떤 Snapshot 실행에 속한 호출인지',
    page_start INT NOT NULL COMMENT 'API 호출 시작 범위',
    page_end INT NOT NULL COMMENT 'API 호출 종료 범위',
    called_at DATETIME NOT NULL COMMENT '실제 API 호출 시각 (Asia/Seoul)',
    status VARCHAR(16) NOT NULL COMMENT '호출 결과: success 또는 error',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_call_date (call_date),
    KEY idx_snapshot (snapshot_id),
    UNIQUE KEY uq_call (snapshot_id, page_start, page_end)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='API 호출 기록 (Budget 계산 단일 진실 소스)';

