from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, max, min

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

datos = [("Enero", 200),
         ("Enero", 450),
         ("Febrero", 300),
         ("Febrero", 700),
         ("Marzo", 500)]

nombre_columnas = ["Mes", "Ventas"]

df = spark.createDataFrame(datos, nombre_columnas)
