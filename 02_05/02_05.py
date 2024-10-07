from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

transacciones_rdd = spark_context.parallelize([
    ("cliente_01", 100),
    ("cliente_02", 200),
    ("cliente_01", 150),
    ("cliente_03", 300),
    ("cliente_02", 250),
    ("cliente_03", 100)
])