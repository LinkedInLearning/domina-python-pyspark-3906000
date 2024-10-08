from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

disenos_rdd = spark_context.parallelize([
    ("DiseñoFloral", 120),
    ("DiseñoGeometrico", 95),
    ("DiseñoAbstracto", 180),
    ("DiseñoFloral", 121)
])