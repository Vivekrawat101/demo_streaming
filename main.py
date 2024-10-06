from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("spark_structure_streaming_demo") \
    .config("spark.ui.port", "4050") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.3") \
    .getOrCreate()

# Define the schema of the input data
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("branch", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("food_item", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_price", DoubleType(), True),
    StructField("payment_mode", StringType(), True)
])

# Read from Kafka
input_kafka_topic = "incoming-order"  # Replace with your input topic
output_kafka_topic = "processed-order"  # Replace with your output topic

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", input_kafka_topic) \
    .load()

# Convert the Kafka message values to strings and parse them as JSON using the defined schema
value_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data"))


# Flatten the nested JSON structure
flattened_df = value_df.select("data.*")

# Perform transformations (e.g., simple filtering here, adjust as needed)
#processed_df = value_df.filter(col("total_price") > 0)
processed_df = flattened_df.withColumn("processed", lit(True))
# Write to another Kafka topic
query = processed_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", output_kafka_topic) \
    .option("checkpointLocation", "/bitnami/kafka/data") \
    .outputMode("append") \
    .start()

# Wait for the termination of the query
query.awaitTermination()
