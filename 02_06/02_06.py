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