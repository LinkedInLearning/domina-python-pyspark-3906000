from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

envios_rdd = spark_context.parallelize([
    ("ZX123", "Envío a Cartago"),
    ("AB456", "Envío a Heredia"),
    ("MN789", "Envío a Alajuela"),
    ("CD234", "Envío a San José"),
    ("XY567", "Envío a Puntarenas")
])