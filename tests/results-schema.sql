CREATE DATABASE IF NOT EXISTS `sp_StatUpdate_testresults`;
USE `sp_StatUpdate_testresults`;

-- One run is one release-gate invocation against one SQL version.
CREATE TABLE runs (
  run_id CHAR(36) NOT NULL PRIMARY KEY,
  commit_sha CHAR(40) NOT NULL,
  tooling_sha256 CHAR(64) NULL,
  orchestration_id VARCHAR(128) NULL,
  sql_version ENUM('2019','2022','2025') NOT NULL,
  sql_build VARCHAR(64) NOT NULL,
  started_at DATETIME(6) NOT NULL,
  ended_at DATETIME(6) NULL,
  duration_ms BIGINT UNSIGNED NOT NULL DEFAULT 0,
  server_alive_at_end BOOLEAN NOT NULL,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

CREATE TABLE suite_runs (
  suite_run_id CHAR(36) NOT NULL PRIMARY KEY,
  run_id CHAR(36) NOT NULL,
  suite_name VARCHAR(128) NOT NULL,
  expected_count INT UNSIGNED NOT NULL,
  actual_count INT UNSIGNED NOT NULL,
  started_at DATETIME(6) NOT NULL,
  duration_ms BIGINT UNSIGNED NOT NULL,
  UNIQUE KEY uq_suite_run (run_id, suite_name),
  CONSTRAINT fk_suite_run FOREIGN KEY (run_id) REFERENCES runs(run_id)
);

CREATE TABLE check_results (
  suite_run_id CHAR(36) NOT NULL,
  check_id VARCHAR(128) NOT NULL,
  result ENUM('PASS','FAIL','NOT_APPLICABLE','SKIPPED') NOT NULL,
  reason TEXT NULL,
  issue_id VARCHAR(64) NULL,
  duration_ms BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (suite_run_id, check_id),
  CONSTRAINT fk_check_suite FOREIGN KEY (suite_run_id) REFERENCES suite_runs(suite_run_id),
  CONSTRAINT ck_nonpass_reason CHECK (result = 'PASS' OR reason IS NOT NULL),
  CONSTRAINT ck_skipped_issue CHECK (result <> 'SKIPPED' OR issue_id IS NOT NULL)
);
