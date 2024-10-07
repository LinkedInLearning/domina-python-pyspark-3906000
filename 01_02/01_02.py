from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MiAplicacionSpark") \
    .getOrCreate()

print("SparkSession creada con éxito")
