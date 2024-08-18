import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegressionModel

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Fraud Detection Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.cassandra.connection.host", "34.29.15.39") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Load the trained model (assuming it was saved with Spark ML)
model = LogisticRegressionModel.load('/savedmodels/logistic_regression_model')

# Define the schema for streaming data
stream_schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True)
])

# Read data from Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "34.29.15.39:9092") \
    .option("subscribe", "transactions") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .withColumn("values_json", from_json(col("value"), stream_schema)) \
    .select("values_json.*")

# Feature Engineering: Define additional features (balance change for orig and dest)
streaming_df = kafka_stream_df.withColumn(
    "balance_change_orig", col("newbalanceOrig") - col("oldbalanceOrg")
).withColumn(
    "balance_change_dest", col("newbalanceDest") - col("oldbalanceDest")
)

# Assemble features
feature_columns = ["amount", "balance_change_orig", "balance_change_dest"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
streaming_df = assembler.transform(streaming_df)

# Apply the trained model for fraud detection
predictions = model.transform(streaming_df)

# Select relevant columns for output
output_df = predictions.select(
    col("step").alias("step"),
    col("amount").alias("amount"),
    col("isFraud").alias("is_fraud"),
    col("prediction").alias("prediction")
)

# Write the predictions to Cassandra
output_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "fraud_detection") \
    .option("table", "predictions") \
    .outputMode("append") \
    .start()

# Optionally, also write the predictions to the console
output_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()
