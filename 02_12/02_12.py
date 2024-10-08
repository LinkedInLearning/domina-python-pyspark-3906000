from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

transacciones_rdd = spark_context.parallelize([100, 200, 300, 400, 500])