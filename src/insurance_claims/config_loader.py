from dataclasses import dataclass
from typing import Optional
from pyspark.sql import SparkSession, DataFrame


@dataclass
class SourceConfig:
    source_name: str
    source_type: str
    source_path: str
    target_catalog: str
    target_schema: str
    target_table: str
    load_type: str
    watermark_column: Optional[str]
    secret_scope: str
    secret_key: str
    is_active: bool
    max_records_per_file: int
    retries: int


def load_active_configs(spark: SparkSession, catalog: str) -> list[SourceConfig]:
    """Reads the control table and returns only active source configs.
    Add a new source by inserting a row here — never edit ingest code."""
    df: DataFrame = spark.table(f"{catalog}.control.ingest_config").filter("is_active = true")
    return [SourceConfig(**row.asDict()) for row in df.collect()]