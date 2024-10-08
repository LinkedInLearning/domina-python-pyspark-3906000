from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AppSpark") \
    .getOrCreate()

datos = [("Juan", 28, "Ingeniero"),
         ("Ana", 23, "Diseñadora"),
         ("Luis", 35, "Arquitecto"),
         ("Carlos",45,"Analista"),
         ("Laura",30,"Desarrolladora"),
         ("Mateo",25,"Diseñador"),
        ("Sofía",38,"Gerente de Proyecto"),
        ("Pedro",50,"Director"),
        ("Elena",27,"Marketing"),
        ("José",33,"Consultor"),
        ("Lucía",29,"Administradora")]

columnas = ["Nombre", "Edad", "Profesión"]

df = spark.createDataFrame(datos, columnas)

df_ordenado = df.sort("Nombre")
df_ordenado.show()

df_ordenado_desc = df.orderBy(df["Edad"].desc()) 
df_ordenado_desc.show()

spark.stop()
