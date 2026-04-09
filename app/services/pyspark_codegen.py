from __future__ import annotations

from app.core.config import Settings
from app.models.schema import ParsedSchema, TableSpec


def generate_pyspark_pipeline_code(parsed_schema: ParsedSchema, domain: str, settings: Settings) -> str:
    del domain
    table_names = [t.name for t in parsed_schema.tables]
    table_list_literal = ", ".join([f'"{name}"' for name in table_names])
    fk_edges = [(t.name, fk.column, fk.ref_table, fk.ref_column) for t in parsed_schema.tables for fk in t.foreign_keys]

    partition_map = {t.name: _infer_partition_column(t) for t in parsed_schema.tables}
    numeric_map = {t.name: _numeric_columns(t) for t in parsed_schema.tables}
    dim_map = {t.name: _dimension_columns(t) for t in parsed_schema.tables}

    return f'''from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "{settings.unity_catalog_name}"
SCHEMA = "{settings.unity_schema_name}"
BRONZE_PATH = "{settings.delta_bronze_path}"
SILVER_PATH = "{settings.delta_silver_path}"
GOLD_PATH = "{settings.delta_gold_path}"
TABLES = [{table_list_literal}]

PARTITION_MAP = {partition_map}
NUMERIC_MAP = {numeric_map}
DIM_MAP = {dim_map}
FK_EDGES = {fk_edges}

spark.sql(f"CREATE CATALOG IF NOT EXISTS {{CATALOG}}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}")

def write_bronze(table_name: str):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"/dbfs/tmp/synthforge/{{table_name}}/*.csv")
    )
    writer = (
        df.withColumn("_ingest_ts", F.current_timestamp())
          .write.mode("overwrite")
          .format("delta")
    )
    pcol = PARTITION_MAP.get(table_name)
    if pcol and pcol in df.columns:
        writer = writer.partitionBy(pcol)
    writer.save(f"{{BRONZE_PATH}}/{{table_name}}")

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}.brz_{{table_name}} "
        f"USING DELTA LOCATION '{{BRONZE_PATH}}/{{table_name}}'"
    )

def write_silver(table_name: str):
    bronze_df = spark.read.format("delta").load(f"{{BRONZE_PATH}}/{{table_name}}")
    cleaned_df = bronze_df.dropDuplicates()
    writer = (
        cleaned_df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .format("delta")
    )
    pcol = PARTITION_MAP.get(table_name)
    if pcol and pcol in cleaned_df.columns:
        writer = writer.partitionBy(pcol)
    writer.save(f"{{SILVER_PATH}}/{{table_name}}")

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}.slv_{{table_name}} "
        f"USING DELTA LOCATION '{{SILVER_PATH}}/{{table_name}}'"
    )

def build_generic_gold():
    # 1) Per-table aggregate gold
    for t in TABLES:
        df = spark.read.format("delta").load(f"{{SILVER_PATH}}/{{t}}")
        dims = [c for c in DIM_MAP.get(t, []) if c in df.columns][:2]
        nums = [c for c in NUMERIC_MAP.get(t, []) if c in df.columns][:4]
        if not nums:
            continue
        aggs = [F.avg(F.col(c)).alias(f"avg_{{c}}") for c in nums] + [F.count("*").alias("row_count")]
        if dims:
            gold_df = df.groupBy(*dims).agg(*aggs)
        else:
            gold_df = df.agg(*aggs)
        out_path = f"{{GOLD_PATH}}/gld_{{t}}_summary"
        gold_df.write.mode("overwrite").format("delta").save(out_path)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}.gld_{{t}}_summary "
            f"USING DELTA LOCATION '{{out_path}}'"
        )

    # 2) FK-join gold (child-parent)
    for child_table, fk_col, parent_table, parent_col in FK_EDGES:
        if child_table not in TABLES or parent_table not in TABLES:
            continue
        cdf = spark.read.format("delta").load(f"{{SILVER_PATH}}/{{child_table}}")
        pdf = spark.read.format("delta").load(f"{{SILVER_PATH}}/{{parent_table}}")
        if fk_col not in cdf.columns or parent_col not in pdf.columns:
            continue
        joined = cdf.join(pdf, cdf[fk_col] == pdf[parent_col], "inner")
        numeric = [c for c in (NUMERIC_MAP.get(child_table, []) + NUMERIC_MAP.get(parent_table, [])) if c in joined.columns][:4]
        if not numeric:
            continue
        dims = [c for c in DIM_MAP.get(child_table, []) if c in joined.columns][:1]
        aggs = [F.avg(F.col(c)).alias(f"avg_{{c}}") for c in numeric] + [F.count("*").alias("row_count")]
        gdf = joined.groupBy(*dims).agg(*aggs) if dims else joined.agg(*aggs)
        out_path = f"{{GOLD_PATH}}/gld_{{child_table}}_{{parent_table}}_joined"
        gdf.write.mode("overwrite").format("delta").save(out_path)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {{CATALOG}}.{{SCHEMA}}.gld_{{child_table}}_{{parent_table}}_joined "
            f"USING DELTA LOCATION '{{out_path}}'"
        )

for t in TABLES:
    write_bronze(t)
    write_silver(t)

build_generic_gold()

# Optional maintenance (safe no-op if not supported by workspace policy)
for t in TABLES:
    try:
        spark.sql(f"OPTIMIZE {{CATALOG}}.{{SCHEMA}}.slv_{{t}}")
    except Exception:
        pass
'''


def _infer_partition_column(table: TableSpec) -> str | None:
    for f in table.fields:
        if f.type in {"date", "datetime"}:
            return f.name
    return None


def _numeric_columns(table: TableSpec) -> list[str]:
    return [f.name for f in table.fields if f.type in {"integer", "float"} and f.name != table.primary_key]


def _dimension_columns(table: TableSpec) -> list[str]:
    dims: list[str] = []
    for f in table.fields:
        n = f.name.lower()
        if f.type == "string" and ("type" in n or "status" in n or "category" in n or "segment" in n):
            dims.append(f.name)
        if f.type == "boolean":
            dims.append(f.name)
    return dims[:3]


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
