"""
SCRIPT TO SEND TRANSACTION LARGE DATASET TO KAFKA THROUGH PRODUCER
"""
'''
Data Sources → Kafka Producers → Kafka Topics → Spark Streaming Consumers → Data Cleaning and Feature Engineering → ML Model Scoring → Flagged Transactions → Stored in Cassandra → Real-Time Alerts → Dashboard and Reporting.
'''

from kafka import KafkaProducer
import json
import csv

# Kafka configuration
bootstrap_servers = '34.29.15.39:9092'
topic_name = 'transactions'

# Create a producer instance
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
)

chunk_size = 10000
chunk = []

with open('/Users/gurmindersingh/Desktop/RESUMES/datasets/transactional_dataset.csv', 'r') as file:
    reader = csv.DictReader(file)  # Use DictReader to directly work with column names
    for i, row in enumerate(reader):
        # we are converting to original format and then sending the JSON format to kafka then we will recieve an cast to schema in spark
        record = {
            "step": int(row["step"]),
            "type": row["type"],
            "amount": float(row["amount"]),
            "nameOrig": row["nameOrig"],
            "oldbalanceOrg": float(row["oldbalanceOrg"]),
            "newbalanceOrig": float(row["newbalanceOrig"]),
            "nameDest": row["nameDest"],
            "oldbalanceDest": float(row["oldbalanceDest"]),
            "newbalanceDest": float(row["newbalanceDest"]),
            "isFraud": int(row["isFraud"]),
            "isFlaggedFraud": int(row["isFlaggedFraud"])
        }
        chunk.append(record)

        if (i + 1) % chunk_size == 0:
            for record in chunk:
                producer.send(topic_name, value=record)
            chunk = []

    if chunk:
        for record in chunk:
            producer.send(topic_name, value=record)

producer.close()
