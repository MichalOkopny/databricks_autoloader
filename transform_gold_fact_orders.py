# Databricks notebook source
from pyspark.sql.functions import col, sum, count, avg, round


def transform_orders(df_final_fact):

    gold_orders = df_final_fact.groupBy("employee_fk", "date_fk").agg(
        count(col("order_id")).alias("total_orders"),
        sum(col("quantity")).alias("total_quantity"),
        round(sum(col("total_price")), 2).alias("dayly_revenue"),
        round(avg(col("unit_price")), 2).alias("avg_unit_price")
    )
    return gold_orders

# COMMAND ----------

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    df_final_fact = spark.table("silver.fact_orders")

    gold_orders = transform_orders(df_final_fact)

    gold_orders.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("gold.orders")