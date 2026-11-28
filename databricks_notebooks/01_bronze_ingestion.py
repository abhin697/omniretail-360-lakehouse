# Databricks notebook source
# MAGIC %run ./00_adls_auth_config

# COMMAND ----------

# DBTITLE 1,Metadata Config (tables_config.json)
from pyspark.sql.functions import current_timestamp, col

config_path = f"{LANDING_PATH}/_metadata/tables_config.json"
tables_df = spark.read.option("multiline", "true").json(config_path)
tables_list = [row.table_name for row in tables_df.collect()]

print(f"Tables discovered from metadata config: {tables_list}")

# COMMAND ----------

# DBTITLE 1,Auto Loader To Incremental Bronze Delta Ingestion
def ingest_to_bronze(table_name: str):
    source_path = f"{LANDING_PATH}/{table_name}/"
    target_path = f"{BRONZE_PATH}/{table_name}"
    schema_path = f"{BRONZE_PATH}/_schemas/{table_name}"
    checkpoint_path = f"{BRONZE_PATH}/_checkpoints/{table_name}"

    print(f"\n--- Ingesting table: {table_name} ---")

    # Auto Loader reads raw CSVs incrementally as strings and tracks processed files
    raw_stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )

    query = (
        raw_stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .start(target_path)
    )
    query.awaitTermination()
    print(f"Completed Bronze ingestion for: {table_name}")

for table in tables_list:
    ingest_to_bronze(table)

# COMMAND ----------

# DBTITLE 1,Verification & Idempotency Check
for table in tables_list:
    df_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/{table}")
    print(f"=== BRONZE: {table.upper()} | Total Rows: {df_bronze.count()} ===")
    display(df_bronze.limit(3))