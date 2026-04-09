from __future__ import annotations

from app.core.config import Settings
from app.models.schema import ParsedSchema


def generate_pyspark_pipeline_code(parsed_schema: ParsedSchema, domain: str, settings: Settings) -> str:
    table_names = [t.name for t in parsed_schema.tables]
    table_list_literal = ", ".join([f'"{name}"' for name in table_names])
    patients_table = "patients" if "patients" in table_names else ""
    claims_table = "claims" if "claims" in table_names else ("encounters" if "encounters" in table_names else "")

    return f'''from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "{settings.unity_catalog_name}"
SCHEMA = "{settings.unity_schema_name}"
BRONZE_PATH = "{settings.delta_bronze_path}"
SILVER_PATH = "{settings.delta_silver_path}"
GOLD_PATH = "{settings.delta_gold_path}"
TABLES = [{table_list_literal}]

# Cost estimate hint:
# - Small cluster (2-4 workers): suitable for <= 1M rows
# - Medium cluster (4-8 workers): recommended for multi-million rows and wider schemas

spark.sql(f"CREATE CATALOG IF NOT EXISTS {{CATALOG}}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}")

def write_bronze(table_name: str):
    # Reads generated files and writes immutable bronze snapshots.
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"/dbfs/tmp/synthforge/{{table_name}}/*.csv")
    )
    (
        df.withColumn("_ingest_ts", F.current_timestamp())
          .write.mode("overwrite")
          .format("delta")
          .save(f"{{BRONZE_PATH}}/{{table_name}}")
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}.brz_{{table_name}} "
        f"USING DELTA LOCATION '{{BRONZE_PATH}}/{{table_name}}'"
    )

def write_silver(table_name: str):
    bronze_df = spark.read.format("delta").load(f"{{BRONZE_PATH}}/{{table_name}}")
    cleaned_df = bronze_df.dropDuplicates()
    (
        cleaned_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .format("delta")
        .save(f"{{SILVER_PATH}}/{{table_name}}")
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}.slv_{{table_name}} "
        f"USING DELTA LOCATION '{{SILVER_PATH}}/{{table_name}}'"
    )

for t in TABLES:
    write_bronze(t)
    write_silver(t)

def write_gold_healthcare():
    pt = spark.read.format("delta").load(f"{{SILVER_PATH}}/{patients_table}")
    cl = spark.read.format("delta").load(f"{{SILVER_PATH}}/{claims_table}")

    patient_key = "id" if "id" in pt.columns else pt.columns[0]
    claim_fk = "patient_id" if "patient_id" in cl.columns else patient_key

    claims_enriched = (
        cl.join(pt, cl[claim_fk] == pt[patient_key], "inner")
          .withColumn(
              "age_group",
              F.when(F.col("age") < 40, F.lit("18-39"))
               .when(F.col("age") < 60, F.lit("40-59"))
               .otherwise(F.lit("60+"))
          )
          .withColumn("cost_per_day", F.col("cost") / F.greatest(F.col("length_of_stay"), F.lit(1)))
    )

    # Silver curated joined table for downstream use.
    (
        claims_enriched.write.mode("overwrite")
        .format("delta")
        .partitionBy("service_date")
        .save(f"{{SILVER_PATH}}/claims_enriched")
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}.slv_claims_enriched "
        f"USING DELTA LOCATION '{{SILVER_PATH}}/claims_enriched'"
    )

    gold_by_age_diag = (
        claims_enriched.groupBy("age_group", "diagnosis_code")
        .agg(
            F.count("*").alias("claim_count"),
            F.avg("cost").alias("avg_cost"),
            F.avg("cost_per_day").alias("avg_cost_per_day"),
            F.avg("readmission_risk_score").alias("avg_readmission_risk")
        )
    )
    (
        gold_by_age_diag.write.mode("overwrite")
        .format("delta")
        .partitionBy("age_group")
        .save(f"{{GOLD_PATH}}/gld_cost_by_age_diagnosis")
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}.gld_cost_by_age_diagnosis "
        f"USING DELTA LOCATION '{{GOLD_PATH}}/gld_cost_by_age_diagnosis'"
    )

    gold_comorbidity = (
        claims_enriched.groupBy("comorbidity_count")
        .agg(
            F.count("*").alias("claim_count"),
            F.avg("length_of_stay").alias("avg_length_of_stay"),
            F.avg("cost").alias("avg_cost"),
            F.avg("readmission_risk_score").alias("avg_readmission_risk")
        )
    )
    (
        gold_comorbidity.write.mode("overwrite")
        .format("delta")
        .save(f"{{GOLD_PATH}}/gld_comorbidity_impact")
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}.gld_comorbidity_impact "
        f"USING DELTA LOCATION '{{GOLD_PATH}}/gld_comorbidity_impact'"
    )

    # Databricks Delta maintenance best-practice.
    spark.sql(f"OPTIMIZE {{CATALOG}}.{{SCHEMA}}.slv_claims_enriched ZORDER BY (patient_id, diagnosis_code)")
    spark.sql(f"OPTIMIZE {{CATALOG}}.{{SCHEMA}}.gld_cost_by_age_diagnosis ZORDER BY (diagnosis_code)")

def write_gold_credit():
    tx = spark.read.format("delta").load(f"{{SILVER_PATH}}/transactions")
    gold = tx.groupBy("is_fraud").agg(
        F.count("*").alias("txn_count"),
        F.avg("amount").alias("avg_amount")
    )
    gold.write.mode("overwrite").format("delta").save(f"{{GOLD_PATH}}/gld_fraud_summary")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}.gld_fraud_summary "
        f"USING DELTA LOCATION '{{GOLD_PATH}}/gld_fraud_summary'"
    )
    spark.sql(f"OPTIMIZE {{CATALOG}}.{{SCHEMA}}.gld_fraud_summary")

if "{domain}" == "healthcare" and "{patients_table}" in TABLES and "{claims_table}" in TABLES:
    write_gold_healthcare()
elif "{domain}" == "credit" and "transactions" in TABLES:
    write_gold_credit()
'''


def generate_kafka_templates(domain: str) -> dict[str, str]:
    producer = f'''from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

topic = "synthforge-{domain}-events"
event = {{"source": "synthforge", "domain": "{domain}", "kind": "synthetic_record"}}

producer.send(topic, event)
producer.flush()
'''

    consumer = f'''from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "synthforge-{domain}-events",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

for message in consumer:
    print(message.value)
'''
    return {"producer.py": producer, "consumer.py": consumer}
