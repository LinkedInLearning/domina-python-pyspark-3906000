from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

compras_rdd = spark_context.parallelize([
    ("Laura", "Electrónica"),
    ("Pedro", "Ropa"),
    ("Laura", "Libros"),
    ("Ana", "Hogar"),
    ("Pedro", "Electrónica"),
    ("Ana", "Libros")
])

categorias_por_cliente_rdd = compras_rdd.groupByKey()

print("Categorías de productos comprados por cliente:")
for cliente, categorias in categorias_por_cliente_rdd.collect():
    print(f"{cliente}: {list(categorias)}")

spark.stop()
