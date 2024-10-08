from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, upper, concat_ws


spark = SparkSession.builder \
    .appName("TransformarColumnas") \
    .getOrCreate()

datos = [("Gen A", "Muestra", 10),
         ("gen b", "Muestra", 20),
         ("gene C", "Muestra", 30)]

nombre_columnas = ["Nombre_Gen", "Tipo", "Valor"]

df = spark.createDataFrame(datos, nombre_columnas)
