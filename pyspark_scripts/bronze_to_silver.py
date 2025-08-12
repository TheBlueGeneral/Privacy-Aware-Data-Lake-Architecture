from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col, lit, year

# --- Spark Session Initialization ---
spark = SparkSession.builder \
    .appName("Bronze to Silver Transformation") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio_storage:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("Spark session created for Bronze to Silver.")

# --- ETL Logic ---
input_path = "s3a://bronze/customers/"
output_path = "s3a://silver/customers_masked/"

# Read data from Bronze Layer
raw_df = spark.read.option("header", "true").csv(input_path)
print(f"Reading from {input_path}")
raw_df.show()

# Apply PII Masking transformations
masked_df = raw_df \
    .withColumn("email_hash", sha2(col("email"), 256)) \
    .withColumn("ssn", lit("REDACTED")) \
    .withColumn("birth_year", year(col("dob")))

# Drop original PII columns to ensure they don't leak to the silver layer
final_df = masked_df.drop("first_name", "last_name", "email", "dob")
print("PII has been masked. Final schema:")
final_df.printSchema()

# Write data to Silver Layer in Parquet format for better performance
final_df.write.mode("overwrite").parquet(output_path)
print(f"Successfully wrote masked data to {output_path}")

spark.stop()