from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.dag import DAG
from datetime import datetime

with DAG(
    dag_id='customer_data_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None, # We will trigger it manually
    catchup=False,
    tags=['data-lake', 'pyspark'],
) as dag:

    # IMPORTANT: We no longer need the "spark.jars.packages" config
    # because the JARs are now built into our custom Spark image.

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver_masking',
        conn_id='spark_default',
        application='/opt/airflow/pyspark_scripts/bronze_to_silver.py',
        conf={"spark.master": "spark://spark-master:7077"},
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold_aggregation',
        conn_id='spark_default',
        application='/opt/airflow/pyspark_scripts/silver_to_gold.py',
        conf={"spark.master": "spark://spark-master:7077"},
    )

    bronze_to_silver >> silver_to_gold