from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, upper, concat_ws


spark = SparkSession.builder \
    .appName("TransformarColumnas") \
    .getOrCreate()

datos = [("Gen A", "Muestra", 10),
         ("gen b", "Muestra", 20),
         ("gen C", "Muestra", 30)]

nombre_columnas = ["Nombre_Gen", "Tipo", "Valor"]

df = spark.createDataFrame(datos, nombre_columnas)

df_transformado = df.withColumn("Nombre_Gen", upper(df["Nombre_Gen"]))

df_transformado = df_transformado.withColumn("Muestra_ID", concat_ws("_", lower(df["Tipo"]), df["Valor"]))

df_transformado.show()

spark.stop()
