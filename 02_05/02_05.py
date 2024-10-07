from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

transacciones_rdd = spark_context.parallelize([
    ("cliente_01", 100),
    ("cliente_02", 200),
    ("cliente_01", 150),
    ("cliente_03", 300),
    ("cliente_02", 250),
    ("cliente_03", 100)
])

total_por_cliente_rdd = transacciones_rdd.reduceByKey(lambda x, y: x + y)

print("Total de transacciones por cliente:")
for cliente, total in total_por_cliente_rdd.collect():
    print(f"{cliente}: {total}")

spark.stop()
