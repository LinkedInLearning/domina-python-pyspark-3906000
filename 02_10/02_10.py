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

primer_registro = rendimiento_rdd.first()
print("Primer registro de rendimiento:", primer_registro)

muestra_registros = rendimiento_rdd.take(3)
print("Muestra de los primeros 3 registros:", muestra_registros)

spark.stop()
