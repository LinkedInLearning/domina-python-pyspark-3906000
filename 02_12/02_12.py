from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

transacciones_rdd = spark_context.parallelize([100, 200, 300, 400, 500])

total_transacciones = transacciones_rdd.reduce(lambda x, y: x + y)
print("Total de transacciones del día:", total_transacciones)

spark.stop()
