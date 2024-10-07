from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

numeros_rdd = spark_context.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

cuadrados_rdd = numeros_rdd.map(lambda x: x ** 2)

print("Números al cuadrado:", cuadrados_rdd.collect())

spark.stop()
