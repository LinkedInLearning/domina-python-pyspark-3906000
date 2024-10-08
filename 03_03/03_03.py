from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

datos = [("Cereal", 100, "Alta"),
         ("Arroz", 150, "Media"),
         ("Avena", 200, "Baja"),
         ("Pan", 120, "Media"),
         ("Leche", 180, "Alta"),
         ("Queso", 140, "Baja"),
         ("Yogurt", 160, "Alta"),
         ("Frutas", 130, "Media"),
         ("Verduras", 110, "Baja"),
         ("Pollo", 210, "Alta"),
         ("Pescado", 190, "Media"),
         ("Huevos", 170, "Alta")]

nombre_columnas = ["Producto", "Cantidad", "Prioridad"]

df = spark.createDataFrame(datos, nombre_columnas)
