from pyspark import pipelines as dp
from pyspark.sql.functions import col

# ============================================================
# Pipeline configuration

catalog_name = spark.conf.get("catalog_name")
bronze_table = f"{catalog_name}.bronze.sales_sdp"
silver_table = f"{catalog_name}.silver.sales_sdp"

# ============================================================
# Silver Streaming Table

@dp.table(name=silver_table)
def sales_silver():

    df = (spark.readStream.table(bronze_table))

    return (
        df.withColumnRenamed("product_name","product")
        .filter(col("transaction_id").isNotNull() & col("store_id").isNotNull())
    )