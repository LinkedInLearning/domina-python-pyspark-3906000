from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext
humedad_rdd = spark_context.parallelize([45, 28, 50, 25, 32, 18, 29, 35, 22, 40])

humedad_critica_rdd = humedad_rdd.filter(lambda x: x < 30)

print("Lecturas de humedad críticas:", humedad_critica_rdd.collect())

spark.stop()
