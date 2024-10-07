from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

datos = [1, 2, 3, 4, 5, 6]
rdd = spark_context.parallelize(datos)
print(rdd.collect())

num_pares = rdd.filter(lambda num: num % 2 == 0)
print(num_pares.collect())

spark.stop()