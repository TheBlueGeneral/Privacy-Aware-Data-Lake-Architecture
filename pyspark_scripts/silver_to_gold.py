from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

# --- Spark Session Initialization ---
spark = SparkSession.builder \
    .appName("Silver to Gold Aggregation") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio_storage:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("Spark session created for Silver to Gold.")

# --- ETL Logic ---
input_path = "s3a://silver/customers_masked/"
output_path = "s3a://gold/customer_analytics/"

# Read cleansed data from Silver Layer
silver_df = spark.read.parquet(input_path)
print(f"Reading from {input_path}")
silver_df.show()

# Aggregate data to create a business-ready view
# Example: Count customers by country
analytics_df = silver_df.groupBy("country") \
    .agg(count("customer_id").alias("customer_count"))

print("Aggregated data:")
analytics_df.show()

# Write aggregated data to Gold Layer
analytics_df.write.mode("overwrite").parquet(output_path)
print(f"Successfully wrote aggregated data to {output_path}")

spark.stop()