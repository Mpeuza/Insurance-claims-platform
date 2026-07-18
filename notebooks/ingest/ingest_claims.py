# Databricks notebook source
dbutils.widgets.text("target_catalog", "test")
dbutils.widgets.text("source_name", "claims")

TARGET_CATALOG = dbutils.widgets.get("target_catalog").strip()
SOURCE_NAME = dbutils.widgets.get("source_name").strip()

from src.insurance_claims.config_loader import load_active_configs

configs = load_active_configs(spark, TARGET_CATALOG)
cfg = next(c for c in configs if c.source_name == SOURCE_NAME)

secret_value = dbutils.secrets.get(scope=cfg.secret_scope, key=cfg.secret_key)

# Placeholder — actual read logic depends on source_type (api/file/jdbc)
df = spark.read.format(cfg.source_type).option("path", cfg.source_path).load()

(df.write
   .format("delta")
   .mode("overwrite" if cfg.load_type == "full" else "append")
   .saveAsTable(f"{cfg.target_catalog}.{cfg.target_schema}.{cfg.target_table}"))