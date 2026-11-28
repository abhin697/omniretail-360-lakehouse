# Databricks notebook source
# MAGIC %run ./00_adls_auth_config

# COMMAND ----------

# DBTITLE 1,Schema Check
for table in ["orders", "customers", "products"]:
    df = spark.read.format("delta").load(f"{BRONZE_PATH}/{table}")
    print(f"\n================ {table.upper()} SCHEMA ================")
    df.printSchema()

# COMMAND ----------

# DBTITLE 1,Imports & Reusable Helper Functions (Quarantine + SCD Type 2 Engine)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def route_quarantine(df_bad, table_name: str):
    """Writes invalid records with quarantine reason for auditability."""
    quarantine_path = f"{SILVER_PATH}/quarantine/{table_name}"
    df_out = df_bad.withColumn("_quarantined_at", F.current_timestamp())
    (
        df_out.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(quarantine_path)
    )
    print(f"[{table_name.upper()}] Quarantined bad rows: {df_out.count()}")

def apply_scd2_merge(clean_df, target_path: str, pk_col: str, tracked_cols: list):
    """Performs atomic SCD Type 2 MERGE INTO using SHA-256 attribute hash comparison."""
    staged_df = (
        clean_df
        .withColumn(
            "record_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in tracked_cols]), 256)
        )
        .withColumn("effective_start_date", F.col("updated_at"))
        .withColumn("effective_end_date", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    if not DeltaTable.isDeltaTable(spark, target_path):
        staged_df.write.format("delta").mode("overwrite").save(target_path)
        print(f"Created initial SCD Type 2 table at {target_path} | Rows: {staged_df.count()}")
        return

    target_table = DeltaTable.forPath(spark, target_path)
    target_active_df = target_table.toDF().filter(F.col("is_current") == True)

    # 1. Brand new records + changed records (merge_key = NULL -> triggers INSERT)
    rows_to_insert = (
        staged_df.alias("s")
        .join(target_active_df.alias("t"), on=pk_col, how="left")
        .filter(F.col(f"t.{pk_col}").isNull() | (F.col("s.record_hash") != F.col("t.record_hash")))
        .select(F.lit(None).cast("string").alias("merge_key"), "s.*")
    )

    # 2. Only changed records (merge_key = pk_col -> triggers UPDATE to expire old active row)
    rows_to_expire = (
        staged_df.alias("s")
        .join(target_active_df.alias("t"), on=pk_col, how="inner")
        .filter(F.col("s.record_hash") != F.col("t.record_hash"))
        .select(F.col(f"s.{pk_col}").alias("merge_key"), "s.*")
    )

    staged_merge_df = rows_to_insert.unionByName(rows_to_expire)

    (
        target_table.alias("target")
        .merge(
            staged_merge_df.alias("source"),
            f"target.{pk_col} = source.merge_key AND target.is_current = true"
        )
        .whenMatchedUpdate(set={
            "is_current": F.lit(False),
            "effective_end_date": F.col("source.updated_at")
        })
        .whenNotMatchedInsert(
            values={c: F.col(f"source.{c}") for c in staged_df.columns}
        )
        .execute()
    )
    print(f"Completed SCD Type 2 MERGE at {target_path}")

# COMMAND ----------

# DBTITLE 1,orders (Cleaning, Business Metrics, Quarantine & Upsert)
raw_orders = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")

typed_orders = (
    raw_orders
    .withColumn("order_id", F.upper(F.trim(F.col("order_id"))))
    .withColumn("customer_id", F.upper(F.trim(F.col("customer_id"))))
    .withColumn("product_id", F.upper(F.trim(F.col("product_id"))))
    .withColumn(
        "order_date",
        F.coalesce(
            F.expr("try_to_date(trim(order_date), 'yyyy-MM-dd')"),
            F.expr("try_to_date(trim(order_date), 'dd/MM/yyyy')"),
            F.expr("try_to_date(trim(order_date), 'dd-MM-yyyy')"),
            F.expr("try_to_date(trim(order_date), 'MM/dd/yyyy')"),
            F.expr("try_to_date(trim(order_date), 'yyyy/MM/dd')")
        )
    )
    .withColumn("quantity", F.expr("try_cast(trim(quantity) as int)"))
    .withColumn("unit_price", F.expr("try_cast(trim(unit_price) as decimal(10,2))"))
    .withColumn("discount_pct", F.coalesce(F.expr("try_cast(trim(discount_pct) as decimal(5,2))"), F.lit(0.00)))
    .withColumn("order_status", F.initcap(F.trim(F.col("order_status"))))
    .withColumn("payment_method", F.upper(F.trim(F.col("payment_method"))))
    .withColumn("shipping_city", F.initcap(F.trim(F.col("shipping_city"))))
    .withColumn(
        "updated_at",
        F.coalesce(
            F.expr("try_cast(trim(updated_at) as timestamp)"),
            F.expr("try_to_timestamp(trim(updated_at), 'dd/MM/yyyy HH:mm:ss')"),
            F.expr("try_to_timestamp(trim(updated_at), 'dd/MM/yyyy')"),
            F.expr("try_to_timestamp(trim(updated_at), 'dd-MM-yyyy HH:mm:ss')"),
            F.expr("try_to_timestamp(trim(updated_at), 'dd-MM-yyyy')"),
            F.col("_ingestion_timestamp")
        )
    )
)

# Data Quality Validation Rules
validated_orders = typed_orders.withColumn(
    "_quarantine_reason",
    F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), "Missing order_id")
     .when(F.col("customer_id").isNull() | F.col("product_id").isNull(), "Missing customer_id or product_id")
     .when(F.col("order_date").isNull(), "Invalid or null order_date")
     .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "Invalid quantity (<= 0)")
     .when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "Invalid unit_price (<= 0)")
     .when((F.col("discount_pct") < 0) | (F.col("discount_pct") > 100), "Invalid discount_pct")
     .otherwise(F.lit(None))
)

route_quarantine(validated_orders.filter(F.col("_quarantine_reason").isNotNull()), "orders")

# Deduplicate valid orders by order_id keeping latest record + compute financial amounts
window_orders = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc(), F.col("_ingestion_timestamp").desc())

clean_orders = (
    validated_orders
    .filter(F.col("_quarantine_reason").isNull())
    .drop("_quarantine_reason", "_rescued_data")
    .withColumn("rn", F.row_number().over(window_orders))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("net_amount", F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100.0), 2))
)

orders_silver_path = f"{SILVER_PATH}/orders"
if not DeltaTable.isDeltaTable(spark, orders_silver_path):
    clean_orders.write.format("delta").mode("overwrite").save(orders_silver_path)
else:
    (
        DeltaTable.forPath(spark, orders_silver_path).alias("t")
        .merge(clean_orders.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
print(f"[ORDERS] Clean Silver Rows: {spark.read.format('delta').load(orders_silver_path).count()}")

# COMMAND ----------

# DBTITLE 1,customers (Cleaning, Quarantine & SCD Type 2)
raw_customers = spark.read.format("delta").load(f"{BRONZE_PATH}/customers")

typed_customers = (
    raw_customers
    .withColumn("customer_id", F.upper(F.trim(F.col("customer_id"))))
    .withColumn("full_name", F.initcap(F.trim(F.col("full_name"))))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("state", F.initcap(F.trim(F.col("state"))))
    .withColumn("loyalty_tier", F.coalesce(F.initcap(F.trim(F.col("loyalty_tier"))), F.lit("Standard")))
    .withColumn("updated_at", F.coalesce(F.expr("try_cast(trim(updated_at) as timestamp)"), F.col("_ingestion_timestamp")))
)

validated_customers = typed_customers.withColumn(
    "_quarantine_reason",
    F.when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "Missing customer_id")
     .when(F.col("full_name").isNull() | (F.col("full_name") == ""), "Missing full_name")
     .when(F.col("email").isNotNull() & (~F.col("email").contains("@")), "Malformed email")
     .otherwise(F.lit(None))
)

route_quarantine(validated_customers.filter(F.col("_quarantine_reason").isNotNull()), "customers")

window_cust = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc(), F.col("_ingestion_timestamp").desc())

clean_customers = (
    validated_customers
    .filter(F.col("_quarantine_reason").isNull())
    .drop("_quarantine_reason", "_rescued_data")
    .withColumn("rn", F.row_number().over(window_cust))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

apply_scd2_merge(
    clean_df=clean_customers,
    target_path=f"{SILVER_PATH}/customers",
    pk_col="customer_id",
    tracked_cols=["full_name", "email", "city", "state", "loyalty_tier"]
)

# COMMAND ----------

# DBTITLE 1,products (Cleaning, Quarantine & SCD Type 2)
raw_products = spark.read.format("delta").load(f"{BRONZE_PATH}/products")

typed_products = (
    raw_products
    .withColumn("product_id", F.upper(F.trim(F.col("product_id"))))
    .withColumn("product_name", F.trim(F.col("product_name")))
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
    .withColumn("sub_category", F.initcap(F.trim(F.col("sub_category"))))
    .withColumn("unit_cost", F.expr("try_cast(trim(unit_cost) as decimal(10,2))"))
    .withColumn("selling_price", F.expr("try_cast(trim(selling_price) as decimal(10,2))"))
    .withColumn("updated_at", F.coalesce(F.expr("try_cast(trim(updated_at) as timestamp)"), F.col("_ingestion_timestamp")))
)

validated_products = typed_products.withColumn(
    "_quarantine_reason",
    F.when(F.col("product_id").isNull() | (F.col("product_id") == ""), "Missing product_id")
     .when(F.col("product_name").isNull() | (F.col("product_name") == ""), "Missing product_name")
     .when(F.col("unit_cost").isNull() | (F.col("unit_cost") < 0), "Invalid unit_cost (< 0)")
     .when(F.col("selling_price").isNull() | (F.col("selling_price") <= 0), "Invalid selling_price (<= 0)")
     .otherwise(F.lit(None))
)

route_quarantine(validated_products.filter(F.col("_quarantine_reason").isNotNull()), "products")

window_prod = Window.partitionBy("product_id").orderBy(F.col("updated_at").desc(), F.col("_ingestion_timestamp").desc())

clean_products = (
    validated_products
    .filter(F.col("_quarantine_reason").isNull())
    .drop("_quarantine_reason", "_rescued_data")
    .withColumn("rn", F.row_number().over(window_prod))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

apply_scd2_merge(
    clean_df=clean_products,
    target_path=f"{SILVER_PATH}/products",
    pk_col="product_id",
    tracked_cols=["product_name", "category", "sub_category", "unit_cost", "selling_price"]
)

# COMMAND ----------

# DBTITLE 1,Silver Audit & Verification Summary
for table in ["orders", "customers", "products"]:
    clean_cnt = spark.read.format("delta").load(f"{SILVER_PATH}/{table}").count()
    quar_cnt = spark.read.format("delta").load(f"{SILVER_PATH}/quarantine/{table}").count()
    print(f"=== SILVER {table.upper()} === Clean Rows: {clean_cnt} | Quarantined Rows: {quar_cnt}")

print("\nSample Silver Customers (SCD Type 2 columns):")
display(spark.read.format("delta").load(f"{SILVER_PATH}/customers").limit(5))