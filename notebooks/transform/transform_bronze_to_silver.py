# Databricks notebook source
dbutils.widgets.text("target_catalog", "test")
TARGET_CATALOG = dbutils.widgets.get("target_catalog").strip()

from pyspark.sql import functions as F

# --- Claims: clean + standardize ---
claims_bronze = spark.table(f"{TARGET_CATALOG}.bronze_insurance_claims.claims")

claims_silver = (
    claims_bronze
    .withColumn("claim_amount", F.col("claim_amount").cast("decimal(12,2)"))
    .withColumn("claim_date", F.to_date("claim_date"))
    .withColumn("status", F.upper(F.trim("status")))
    .filter(F.col("claim_id").isNotNull())
    .dropDuplicates(["claim_id"])
)

(claims_silver.write
   .format("delta")
   .mode("overwrite")
   .option("mergeSchema", "true")
   .saveAsTable(f"{TARGET_CATALOG}.silver_insurance_claims.claims"))

# --- Policies: clean + standardize ---
policies_bronze = spark.table(f"{TARGET_CATALOG}.bronze_insurance_claims.policies")

policies_silver = (
    policies_bronze
    .withColumn("premium_amount", F.col("premium_amount").cast("decimal(12,2)"))
    .withColumn("policy_start_date", F.to_date("policy_start_date"))
    .withColumn("policy_end_date", F.to_date("policy_end_date"))
    .filter(F.col("policy_id").isNotNull())
    .dropDuplicates(["policy_id"])
)

(policies_silver.write
   .format("delta")
   .mode("overwrite")
   .option("mergeSchema", "true")
   .saveAsTable(f"{TARGET_CATALOG}.silver_insurance_claims.policies"))

print(f"Silver load complete: {claims_silver.count()} claims, {policies_silver.count()} policies")