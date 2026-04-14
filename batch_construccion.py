from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("BatchConstruccion") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Cargar dataset
df = spark.read.csv("dataset_cosntruccion.csv", header=True, inferSchema=True)

print("=== ESQUEMA ===")
df.printSchema()

print("=== PRIMERAS FILAS ===")
df.show(5, truncate=False)

# Limpieza
df_clean = df.dropna(subset=["MUN-COMERCIAL", "EST-MATRICULA"])
df_clean = df_clean.dropDuplicates()

print("=== TOTAL REGISTROS LIMPIOS ===")
print(df_clean.count())

# Selección
df_sel = df_clean.select(
    col("MUN-COMERCIAL").alias("municipio"),
    col("EST-MATRICULA").alias("estado")
)

# Conteos
print("=== CONTEO POR MUNICIPIO ===")
df_sel.groupBy("municipio").count().orderBy(col("count").desc()).show(truncate=False)

print("=== CONTEO POR ESTADO ===")
df_sel.groupBy("estado").count().orderBy(col("count").desc()).show(truncate=False)
