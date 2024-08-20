from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Iceberg with MinIO and Hive Metastore") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.spark_catalog.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://wh1/warehouse/") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars", "/opt/mage/jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar,/opt/mage/jars/iceberg-core-1.6.0.jar,/opt/mage/jars/hadoop-aws-3.3.4.jar,/opt/mage/jars/aws-java-sdk-bundle-1.12.341.jar") \
        .enableHiveSupport() \
        .getOrCreate()


    # spark.sql("""
    #     CREATE TABLE spark_catalog.default.example_table (
    #         id STRING,
    #         name STRING
    #     )
    #     USING iceberg
    # """)

    spark.sql('SHOW TABLES IN spark_catalog.default;').show()

    spark.stop()

if __name__ == "__main__":
    main()
