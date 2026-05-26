# Databricks notebook source
# ==========================================================
# Notebook Name: NB_Electronics_Delta_Processing
# Project: Electronics Store Azure Data Engineering Project
# Purpose: Raw to Bronze, Silver, Gold Delta Lake Processing
# ==========================================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Notebook Parameters
dbutils.widgets.text("raw_path", "/mnt/adls/raw/electronics_sales/")
dbutils.widgets.text("bronze_path", "/mnt/adls/bronze/electronics_sales/")
dbutils.widgets.text("silver_path", "/mnt/adls/silver/electronics_sales/")
dbutils.widgets.text("gold_path", "/mnt/adls/gold/electronics_sales/")

raw_path = dbutils.widgets.get("raw_path")
bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")
gold_path = dbutils.widgets.get("gold_path")

# COMMAND ----------

# DBTITLE 1,Read Raw Data From ADLS
raw_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(raw_path)

display(raw_df)

# COMMAND ----------

# DBTITLE 1,Bronze Layer - Raw Delta
bronze_df = raw_df \
    .withColumn("source_system", lit("OnPrem_SQL_Server")) \
    .withColumn("ingestion_timestamp", current_timestamp())

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(bronze_path)

# COMMAND ----------

# DBTITLE 1,Read Bronze Delta
bronze_delta_df = spark.read.format("delta").load(bronze_path)

# COMMAND ----------

# DBTITLE 1,Silver Layer - Cleaning and Business Transformation
silver_df = bronze_delta_df \
    .dropDuplicates(["order_id"]) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("quantity") > 0) \
    .filter(col("unit_price") > 0) \
    .withColumn("sales_amount", round(col("quantity") * col("unit_price"), 2)) \
    .withColumn("order_date", to_date(col("order_date"))) \
    .withColumn("order_year", year(col("order_date"))) \
    .withColumn("order_month", month(col("order_date"))) \
    .withColumn(
        "sales_category",
        when(col("sales_amount") >= 50000, "High Value")
        .when(col("sales_amount") >= 20000, "Medium Value")
        .otherwise("Low Value")
    ) \
    .withColumn("processed_timestamp", current_timestamp())

display(silver_df)

# COMMAND ----------

# DBTITLE 1,Write Silver Delta Layer
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .save(silver_path)

# COMMAND ----------

# DBTITLE 1,Gold Layer - City Sales Summary
city_sales_df = silver_df.groupBy("city") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_quantity"),
        round(sum("sales_amount"), 2).alias("total_sales"),
        round(avg("sales_amount"), 2).alias("avg_order_value")
    ) \
    .orderBy(col("total_sales").desc())

display(city_sales_df)

city_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path + "city_sales_summary/")

# COMMAND ----------

# DBTITLE 1,Gold Layer - Product Sales Summary
product_sales_df = silver_df.groupBy("category", "product_name") \
    .agg(
        count("order_id").alias("total_orders"),
        sum("quantity").alias("total_units_sold"),
        round(sum("sales_amount"), 2).alias("total_revenue")
    ) \
    .orderBy(col("total_revenue").desc())

display(product_sales_df)

product_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path + "product_sales_summary/")

# COMMAND ----------

# DBTITLE 1,Gold Layer - Customer Sales Summary
customer_sales_df = silver_df.groupBy("customer_id", "customer_name") \
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("sales_amount"), 2).alias("customer_total_sales"),
        round(avg("sales_amount"), 2).alias("avg_purchase_value")
    ) \
    .orderBy(col("customer_total_sales").desc())

display(customer_sales_df)

customer_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path + "customer_sales_summary/")

# COMMAND ----------

# DBTITLE 1,Top Product by City Using Window Function
window_spec = Window.partitionBy("city").orderBy(col("sales_amount").desc())

top_product_city_df = silver_df.withColumn(
    "sales_rank",
    dense_rank().over(window_spec)
).filter(col("sales_rank") == 1)

display(top_product_city_df.select(
    "city",
    "product_name",
    "category",
    "sales_amount",
    "sales_rank"
))

top_product_city_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path + "top_product_by_city/")

# COMMAND ----------

print("NB_Electronics_Delta_Processing completed successfully.")
