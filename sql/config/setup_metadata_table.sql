CREATE SCHEMA IF NOT EXISTS ${catalog}.control;

CREATE TABLE IF NOT EXISTS ${catalog}.control.ingest_config (
  source_name          STRING,
  source_type          STRING,
  source_path          STRING,
  target_catalog       STRING,
  target_schema        STRING,
  target_table         STRING,
  load_type            STRING,
  watermark_column      STRING,
  secret_scope          STRING,
  secret_key            STRING,
  is_active             BOOLEAN,
  max_records_per_file  BIGINT,
  retries               INT
);