from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

titulos_libros = [
    "En un lugar de la Mancha de cuyo nombre no quiero acordarme",
    "No hace mucho tiempo en una galaxia muy muy lejana",
    "Era el mejor de los tiempos, era el peor de los tiempos"
]

libros_rdd = spark_context.parallelize(titulos_libros)