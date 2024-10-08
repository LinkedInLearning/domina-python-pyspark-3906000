from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

observaciones_rdd = spark_context.parallelize([
    ("Eclipse", 90),
    ("Cometa", 70),
    ("Eclipse", 85),
    ("Supernova", 95),
    ("Cometa", 75)
])
