# Databricks notebook source
# MAGIC %run ./00_adls_auth_config

# COMMAND ----------

# DBTITLE 1,dim_customers
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType

silver_customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")
w_cust_sk = Window.orderBy("customer_id", "effective_start_date")

unknown_cust = spark.createDataFrame(
    [(-1, "UNKNOWN", "Unknown Customer", "unknown@omniretail.local", "Unknown", "Unknown", "Standard", None, None, True)],
    schema=StructType([
        StructField("customer_sk", IntegerType(), False),
        StructField("customer_id", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("loyalty_tier", StringType(), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_current", BooleanType(), True),
    ])
)

dim_customers = unknown_cust.unionByName(
    silver_customers
    .withColumn("customer_sk", F.row_number().over(w_cust_sk))
    .select("customer_sk", "customer_id", "full_name", "email", "city", "state", "loyalty_tier", "effective_start_date", "effective_end_date", "is_current")
)

dim_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/dim_customers")
print(f"[GOLD] dim_customers updated (with SK=-1) | Rows: {dim_customers.count()}")

# COMMAND ----------

# DBTITLE 1,dim_products
from pyspark.sql.types import DecimalType

silver_products = spark.read.format("delta").load(f"{SILVER_PATH}/products")
w_prod_sk = Window.orderBy("product_id", "effective_start_date")

unknown_prod = spark.createDataFrame(
    [(-1, "UNKNOWN", "Unknown Product", "Unassigned", "Unassigned", None, None, None, None, None, True)],
    schema=StructType([
        StructField("product_sk", IntegerType(), False),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("unit_cost", DecimalType(10, 2), True),
        StructField("selling_price", DecimalType(10, 2), True),
        StructField("unit_margin", DecimalType(11, 2), True),
        StructField("effective_start_date", TimestampType(), True),
        StructField("effective_end_date", TimestampType(), True),
        StructField("is_current", BooleanType(), True),
    ])
)

dim_products = unknown_prod.unionByName(
    silver_products
    .withColumn("product_sk", F.row_number().over(w_prod_sk))
    .withColumn("unit_margin", F.round(F.col("selling_price") - F.col("unit_cost"), 2))
    .select("product_sk", "product_id", "product_name", "category", "sub_category", "unit_cost", "selling_price", "unit_margin", "effective_start_date", "effective_end_date", "is_current")
)

dim_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/dim_products")
print(f"[GOLD] dim_products updated (with SK=-1) | Rows: {dim_products.count()}")

# COMMAND ----------

# DBTITLE 1,dim_date
silver_orders = spark.read.format("delta").load(f"{SILVER_PATH}/orders")

date_bounds = silver_orders.select(
    F.min("order_date").alias("min_date"),
    F.max("order_date").alias("max_date")
).collect()[0]

dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{date_bounds.min_date}'), to_date('{date_bounds.max_date}'), interval 1 day)) AS full_date")
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend", F.when(F.dayofweek("full_date").isin([1, 7]), True).otherwise(False))
    .select("date_key", "full_date", "year", "quarter", "month", "month_name", "day_of_month", "day_name", "is_weekend")
)

dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/dim_date")
print(f"[GOLD] dim_date created | Rows: {dim_date.count()} ({date_bounds.min_date} to {date_bounds.max_date})")

# COMMAND ----------

# DBTITLE 1,fact_orders
o = silver_orders.alias("o")
dc_pit = dim_customers.alias("dc_pit")
dc_cur = dim_customers.filter(F.col("is_current") == True).alias("dc_cur")
dp_pit = dim_products.alias("dp_pit")
dp_cur = dim_products.filter(F.col("is_current") == True).alias("dp_cur")

resolved_cost_expr = F.coalesce(F.col("dp_pit.unit_cost"), F.col("dp_cur.unit_cost"), F.lit(0.00))
cost_amount_expr = F.round(F.col("o.quantity") * resolved_cost_expr, 2)
profit_amount_expr = F.round(F.col("o.net_amount") - cost_amount_expr, 2)

fact_orders = (
    o
    # Point-in-time SCD2 join + current record fallback + Unknown (-1) fallback
    .join(
        dc_pit,
        (F.col("o.customer_id") == F.col("dc_pit.customer_id")) &
        (F.col("o.order_date") >= F.to_date(F.col("dc_pit.effective_start_date"))) &
        (F.col("dc_pit.effective_end_date").isNull() | (F.col("o.order_date") < F.to_date(F.col("dc_pit.effective_end_date")))),
        "left"
    )
    .join(dc_cur, F.col("o.customer_id") == F.col("dc_cur.customer_id"), "left")
    .join(
        dp_pit,
        (F.col("o.product_id") == F.col("dp_pit.product_id")) &
        (F.col("o.order_date") >= F.to_date(F.col("dp_pit.effective_start_date"))) &
        (F.col("dp_pit.effective_end_date").isNull() | (F.col("o.order_date") < F.to_date(F.col("dp_pit.effective_end_date")))),
        "left"
    )
    .join(dp_cur, F.col("o.product_id") == F.col("dp_cur.product_id"), "left")
    .select(
        F.col("o.order_id"),
        F.date_format(F.col("o.order_date"), "yyyyMMdd").cast("int").alias("date_key"),
        F.col("o.order_date"),
        F.coalesce(F.col("dc_pit.customer_sk"), F.col("dc_cur.customer_sk"), F.lit(-1)).alias("customer_sk"),
        F.coalesce(F.col("dp_pit.product_sk"), F.col("dp_cur.product_sk"), F.lit(-1)).alias("product_sk"),
        F.col("o.order_status"),
        F.col("o.payment_method"),
        F.col("o.shipping_city"),
        F.col("o.quantity"),
        F.col("o.unit_price"),
        F.col("o.discount_pct"),
        F.col("o.gross_amount"),
        F.col("o.net_amount"),
        cost_amount_expr.alias("cost_amount"),
        profit_amount_expr.alias("profit_amount")
    )
)

fact_orders_path = f"{GOLD_PATH}/fact_orders"
fact_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(fact_orders_path)

# Delta Lake file compaction & Z-Order indexing for fast analytical queries
spark.sql(f"OPTIMIZE delta.`{fact_orders_path}` ZORDER BY (order_date, customer_sk)")
print(f"[GOLD] fact_orders created & Z-Ordered | Rows: {fact_orders.count()}")

# COMMAND ----------

# DBTITLE 1,Gold Star Schema Verification & Executive Business KPIs
fact_df = spark.read.format("delta").load(f"{GOLD_PATH}/fact_orders")
prod_df = spark.read.format("delta").load(f"{GOLD_PATH}/dim_products")
cust_df = spark.read.format("delta").load(f"{GOLD_PATH}/dim_customers")

# 1. Referential Integrity Check (Must be 0 NULL surrogate keys)
null_sks = fact_df.filter(F.col("customer_sk").isNull() | F.col("product_sk").isNull()).count()
print(f"Referential Integrity Check -> Unmatched Surrogate Keys: {null_sks}")

# 2. Category-wise Revenue & Profit Summary
kpi_category = (
    fact_df.alias("f")
    .join(prod_df.alias("p"), "product_sk")
    .groupBy("p.category")
    .agg(
        F.count("f.order_id").alias("total_orders"),
        F.sum("f.quantity").alias("units_sold"),
        F.round(F.sum("f.net_amount"), 2).alias("total_revenue"),
        F.round(F.sum("f.profit_amount"), 2).alias("total_profit")
    )
    .orderBy(F.col("total_revenue").desc())
)

print("\n=== GOLD EXECUTIVE KPI: REVENUE & PROFIT BY CATEGORY ===")
display(kpi_category)