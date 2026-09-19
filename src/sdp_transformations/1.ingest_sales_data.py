from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


# ============================================================
# Pipeline configuration

catalog_name = spark.conf.get("catalog_name")
bronze_table = f"{catalog_name}.bronze.sales_sdp"
input_path = f"/Volumes/{catalog_name}/landing/raw_sales_data/"

# ============================================================
# Bronze Streaming Table

@dp.table(name=bronze_table)
def sales_bronze():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(input_path).withColumn("load_timestamp",current_timestamp())
    )