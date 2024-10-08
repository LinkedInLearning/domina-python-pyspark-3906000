from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

rendimiento_rdd = spark_context.parallelize([
    ("Dron_01", 98),
    ("Dron_02", 95),
    ("Dron_03", 99),
    ("Dron_04", 92),
    ("Dron_05", 97)
])
