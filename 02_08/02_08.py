from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

clientes_rdd = spark_context.parallelize([
    ("C001", "Ana"),
    ("C002", "Carlos"),
    ("C003", "Lucía"),
    ("C004", "Pedro")
])

contratos_rdd = spark_context.parallelize([
    ("C001", "Contrato Luz"),
    ("C002", "Contrato Televisión"),
    ("C003", "Contrato Internet"),
    ("C005", "Contrato Telefonía")
])

clientes_contratos_rdd = clientes_rdd.join(contratos_rdd)

print("Clientes y sus contratos combinados:")
for cliente_id, (nombre, contrato) in clientes_contratos_rdd.collect():
    print(f"ID: {cliente_id}, Nombre: {nombre}, Contrato: {contrato}")

spark.stop()
