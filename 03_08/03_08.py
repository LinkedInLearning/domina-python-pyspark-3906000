from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

clientes = [("1", "Juan", "España"),
            ("2", "Ana", "México"),
            ("3", "Luis", "Colombia")]

compras = [("1", "Teléfono", 1000),
           ("2", "Computadora", 1500),
           ("1", "Monitor", 550), 
           ("4", "Audífonos", 360)]

df_clientes = spark.createDataFrame(clientes, ["Cliente_ID", "Nombre", "País"])
df_compras = spark.createDataFrame(compras, ["Cliente_ID", "Producto", "Monto"])

df_join = df_clientes.join(df_compras, on="Cliente_ID", how="inner")

df_join.show()

spark.stop()
