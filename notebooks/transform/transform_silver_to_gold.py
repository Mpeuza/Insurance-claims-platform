# Databricks notebook source
dbutils.widgets.text("target_catalog", "test")
TARGET_CATALOG = dbutils.widgets.get("target_catalog").strip()

from pyspark.sql import functions as F

claims = spark.table(f"{TARGET_CATALOG}.silver_insurance_claims.claims")
policies = spark.table(f"{TARGET_CATALOG}.silver_insurance_claims.policies")

# --- Fact table: claims joined to policy context ---
fact_claims = (
    claims.alias("c")
    .join(policies.alias("p"), F.col("c.policy_id") == F.col("p.policy_id"), "left")
    .select(
        "c.claim_id", "c.policy_id", "c.claim_date", "c.claim_amount", "c.status",
        "p.premium_amount", "p.policy_start_date", "p.policy_end_date"
    )
    # simple fraud heuristic placeholder — claim filed within 14 days of policy start
    .withColumn(
        "early_claim_flag",
        F.when(F.datediff("c.claim_date", "p.policy_start_date") < 14, True).otherwise(False)
    )
)

(fact_claims.write
   .format("delta")
   .mode("overwrite")
   .option("mergeSchema", "true")
   .saveAsTable(f"{TARGET_CATALOG}.gold_insurance_claims.fact_claims"))

# --- Aggregate: loss ratio by month ---
loss_ratio = (
    fact_claims
    .withColumn("claim_month", F.date_trunc("month", "claim_date"))
    .groupBy("claim_month")
    .agg(
        F.sum("claim_amount").alias("total_claims"),
        F.sum("premium_amount").alias("total_premium"),
        F.count("claim_id").alias("claim_count"),
        F.sum(F.col("early_claim_flag").cast("int")).alias("early_claims_count")
    )
    .withColumn("loss_ratio", F.round(F.col("total_claims") / F.col("total_premium"), 3))
)

(loss_ratio.write
   .format("delta")
   .mode("overwrite")
   .saveAsTable(f"{TARGET_CATALOG}.gold_insurance_claims.agg_monthly_loss_ratio"))

print("Gold layer built: fact_claims, agg_monthly_loss_ratio")