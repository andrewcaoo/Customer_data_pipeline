from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Iceberg with MinIO and Hive Metastore") \
    .getOrCreate()

# Test table creation
spark.sql("CREATE TABLE spark_catalog.default.test_table (id INT, data STRING) USING iceberg")
spark.sql("SHOW TABLES IN spark_catalog.default").show()
