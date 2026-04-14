from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

df_value = df.selectExpr("CAST(value AS STRING) as mensaje")

df_parsed = df_value.select(
    split(col("mensaje"), ",").getItem(0).alias("municipio"),
    split(col("mensaje"), ",").getItem(1).alias("estado")
)

df_count = df_parsed.groupBy("municipio", "estado").count()

query = df_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
