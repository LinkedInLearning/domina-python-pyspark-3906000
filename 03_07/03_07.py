from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

datos = [("Juan", 28),
         ("Ana", 23),
         ("Luis", 35),
         ("Carlos",45),
         ("Laura",30),
         ("Mateo",25),
        ("Sofía",38),
        ("Pedro",50),
        ("Elena",27),
        ("José",33),
        ("Lucía",29)]

nombre_columnas = ["Nombre", "Horas"]

df = spark.createDataFrame(datos, nombre_columnas)

df_condicional = df.withColumn("Horas Extra", when(df["Horas"] > 40, df["Horas"] -  40).otherwise(0))
df_condicional.show()

spark.stop()
