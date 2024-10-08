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

print("Diseños colectados: ", disenos_rdd.collect())

disenos_diccionario = disenos_rdd.collectAsMap()
print("Diseños como diccionario: ", disenos_diccionario)

spark.stop()
