from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

linea1_rdd = spark_context.parallelize(["ChipA", "ChipB", "ChipC", "ChipF"])
linea2_rdd = spark_context.parallelize(["ChipD", "ChipE", "ChipF"])

union_rdd = linea1_rdd.union(linea2_rdd)

print("Chips producidos en ambas líneas: ", union_rdd.collect())

spark.stop()
