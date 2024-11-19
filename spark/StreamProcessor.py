from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def start_streaming():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("SpotifyKafkaStreaming") \
        .getOrCreate()

    # Define schema based on Spotify data
    schema = StructType([
        StructField("track_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("popularity", StringType(), True),
        StructField("artist_name", StringType(), True)
    ])

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "test_sales") \
        .load()

    # Transform and parse JSON
    spotify_data = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Write to console for real-time monitoring
    # query = spotify_data.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .start()

    # For saving to HDFS, uncomment below lines
    query = spotify_data.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/home/hadoop/cs424_project/hdfs/checkpoints") \
        .option("path", "hdfs://localhost:50070/kafka/hdfs/spotify_data") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()
