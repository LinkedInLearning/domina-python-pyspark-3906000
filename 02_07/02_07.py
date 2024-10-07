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

envios_ordenados_rdd = envios_rdd.sortByKey()

print("Envíos ordenados por código de seguimiento de manera ascendente:")
for codigo, detalle in envios_ordenados_rdd.collect():
    print(f"{codigo}: {detalle}")

spark.stop()
