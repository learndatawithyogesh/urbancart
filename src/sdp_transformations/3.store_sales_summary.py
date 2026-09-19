from pyspark import pipelines as dp
from pyspark.sql.functions import (col,sum,countDistinct)

# ============================================================
# Pipeline configuration
catalog_name = spark.conf.get("catalog_name")
silver_table = f"{catalog_name}.silver.sales_sdp"
gold_table = f"{catalog_name}.gold.store_sales_summary_sdp"


# ============================================================
# Store Sales Summary

@dp.materialized_view(name=gold_table)
def store_sales_summary():

    df = (spark.read.table(silver_table))

    return (
        df.withColumn("sales_amount",col("quantity") * col("unit_price"))
        .groupBy("sales_date","store_id")
        .agg(
            countDistinct("transaction_id").alias("total_transactions"),
            sum("quantity").alias("total_quantity"),
            sum("sales_amount").alias("total_sales")
        )
    )