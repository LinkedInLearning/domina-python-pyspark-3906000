from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

spark_context = spark.sparkContext

ruta_archivo = "datos.csv"

rdd = spark_context.textFile(ruta_archivo)

if rdd.isEmpty():
    print("El RDD está vacío. Verifica el contenido del archivo.")
else:
    fila_encabezado = rdd.first()

    datos_sin_encabezado = rdd.filter(lambda fila: fila != fila_encabezado)
    datos_sin_encabezado = datos_sin_encabezado.map(lambda fila: fila.split(","))

    print("Primeros 5 registros:")
    for registro in datos_sin_encabezado.take(5):
        print(registro)

spark.stop()