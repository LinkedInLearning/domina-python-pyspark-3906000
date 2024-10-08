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

df_agg = df.groupBy("Mes").agg(
    sum("Ventas").alias("TotalVentas"),
    avg("Ventas").alias("PromedioVentas"),
    max("Ventas").alias("MaxVenta"),
    min("Ventas").alias("MinVenta")
)

df_agg.show()

spark.stop()
