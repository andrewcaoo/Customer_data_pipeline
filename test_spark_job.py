from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import pandas as pd
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, to_timestamp

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(wh_table, *args, **kwargs):

    spark = SparkSession.builder \
        .appName("Iceberg, MinIO and Hive") \
        .master('spark://spark:7077') \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.spark_catalog.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://wh1/iceberg/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.extraClassPath", "/opt/mage/jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar:/opt/mage/jars/iceberg-core-1.6.0.jar:/opt/mage/jars/hadoop-aws-3.3.4.jar:/opt/mage/jars/aws-java-sdk-bundle-1.12.341.jar") \
        .config("spark.executor.extraClassPath", "/opt/mage/jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar:/opt/mage/jars/iceberg-core-1.6.0.jar:/opt/mage/jars/hadoop-aws-3.3.4.jar:/opt/mage/jars/aws-java-sdk-bundle-1.12.341.jar") \
        .enableHiveSupport() \
        .getOrCreate()

    # spark_dfs_dict = {table_name: spark.createDataFrame(df) for table_name, df in dataframes_dict.items()}
    spark.sql("CREATE DATABASE IF NOT EXISTS user_wh")

    spark.sql("""
        DROP TABLE IF EXISTS user_wh.dim_user_info;
    """)
    spark.sql("""
        DROP TABLE IF EXISTS user_wh.dim_employment;
    """)
    spark.sql("""
        DROP TABLE IF EXISTS user_wh.dim_address;
    """)
    spark.sql("""
        DROP TABLE IF EXISTS user_wh.dim_subscription;
    """)


    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS user_wh.dim_user_info (
            id INT,
            uid STRING,
            password STRING,
            first_name STRING,
            last_name STRING,
            username STRING,
            email STRING,
            avatar STRING,
            gender STRING,
            phone_number STRING,
            social_insurance_number STRING,
            date_of_birth STRING,
            credit_card_cc_number STRING,
            row_id STRING
        )
        USING iceberg
        LOCATION 's3a://wh1/iceberg/warehouse/user_wh/dim_user_info';
    """)

    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS user_wh.dim_employment (
            employment_title STRING,
            employment_key_skill STRING,
            row_id STRING
        )
        USING iceberg
        LOCATION 's3a://wh1/iceberg/warehouse/user_wh/dim_employment';
    """)

    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS user_wh.dim_address (
            address_city STRING,
            address_street_name STRING,
            address_street_address STRING,
            address_zip_code STRING,
            address_state STRING,
            address_country STRING,
            address_coordinates_lat DOUBLE,
            address_coordinates_lng DOUBLE,
            row_id STRING
        )
        USING iceberg
        LOCATION 's3a://wh1/iceberg/warehouse/user_wh/dim_address';
    """)

    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS user_wh.dim_subscription (
            subscription_plan STRING,
            subscription_status STRING,
            subscription_payment_method STRING,
            subscription_term STRING,
            row_id STRING
        )
        USING iceberg
        LOCATION 's3a://wh1/iceberg/warehouse/user_wh/dim_subscription';
    """)

    spark.sql("REFRESH TABLE user_wh.dim_user_info")


    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("uid", StringType(), True),
        StructField("password", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("username", StringType(), True),
        StructField("email", StringType(), True),
        StructField("avatar", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("social_insurance_number", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("credit_card_cc_number", StringType(), True),
        StructField("row_id", StringType(), True)
    ])
    
    spark_df = spark.createDataFrame(wh_table['dim_user_info'], schema)
    # spark_df = spark_df.withColumn("date_of_birth", to_timestamp(col("date_of_birth"), "yyyy-MM-dd'T'HH:mm:ss"))
    # Define the Iceberg table location
    table_name = "user_wh.dim_user_info"

    # Write the Spark DataFrame to the Iceberg table
    spark_df.write.format("iceberg") \
        .mode("overwrite") \
        .save(f"s3a://wh1/iceberg/warehouse/{table_name}")

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
