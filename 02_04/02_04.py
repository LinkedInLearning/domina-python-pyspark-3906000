from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

linea1_rdd = spark_context.parallelize(["ChipA", "ChipB", "ChipC"])
linea2_rdd = spark_context.parallelize(["ChipD", "ChipE", "ChipF"])