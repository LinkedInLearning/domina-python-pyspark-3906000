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

print("Primer registro de observación:", observaciones_rdd.first())

print("Total de observaciones: ", observaciones_rdd.count())

print("Conteo por fenómeno: ", observaciones_rdd.countByKey())

spark.stop()
