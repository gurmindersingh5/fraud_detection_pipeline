Fraud Detection Pipeline Using Spark Streaming, Kafka, and Cassandra

This project is a real-time fraud detection pipeline that uses Apache Spark Streaming to process transactional data coming 
from Kafka, applies a pre-trained machine learning model to predict fraudulent transactions, and writes the predictions to 
Cassandra. The pipeline is designed to be scalable and efficient, capable of handling large volumes of streaming data with 
low latency.

Table of Contents:
-----------------
1. Architecture
2. Technologies used
3. Setup Instructions
4. Running the Pipeline
5. Challenges Faced


ARCHITECTURE:
------------
<img width="786" alt="Screenshot 2024-08-18 at 7 25 07 PM" src="https://github.com/user-attachments/assets/4297906f-29d6-4144-a7d7-e1c6d52fd955">

1. Kafka: Acts as a message broker, streaming transactional data to the Spark job.
2. Spark Streaming: Processes the real-time data, performs feature engineering, and applies a machine learning model to detect fraud.
3. Cassandra: Stores the predictions made by the model, allowing for fast retrieval and analysis.
4. Pre-Trained Machine Learning Model: A Logistic Regression model trained to detect fraudulent transactions.


TECHNOLOGIES USED:
-----------------
1. Python3 env.
2. Kafka (why kafka: to provide a real-time stream of transaction data as normal APIs would give discontinued stream)
3. Spark (Spark stream will efficiently process the high volume of data in micro batches ) 
4. Docker (Used to deploy) 
5. LogisticRegression pre-trained Model (For now using logisticregression model only)
6. Cassandra db (to store raw hist data and scored data)


SETUP INTRUCTIONS:
-----------------
Prerequisites:
1. Java 8: Required for running Apache Spark.
2. Apache Spark 3.4.0: Install Spark on your local machine or cluster.
3. Apache Kafka: Set up Kafka and create the required topics.
4. Cassandra: Install Cassandra using Docker and create the necessary keyspace and tables.
5. Python 3.x: Install Python along with the required libraries.


RUNNING THE PIPELINE:
--------------------
1. Start Zookeeper using script.sh.
2. Start the Kafka broker and ensure the transactions topic is ready to receive messages.
3. Run the Spark Streaming Job. Ensure the Spark has connected to Kafka.
4. Run the producer script to simulate the transactions which will send to kafka topic.
5. Monitor the Output.
6. You can monitor the predictions being written to Cassandra using cqlsh: SELECT * FROM fraud_detection.predictions;


CHALLENGES FACED:
----------------
1.
Versions mismatch:
Latest Java versions are not compatible with Spark, So needed to downgrade to Java8.
Use same version for PySpark as Spark
Spark version 3.4 or later required to avoid error 'SupportTriggerAvailableNow'

3.
Jar Libraries required:
Jar libraries required for Kafka and Cassandra in order to connect to Spark which can be installed while creating Spark 
application.

4.
Schema was not able to be casted by Spark:
Becasue of the fact that we need to produce accurate format of datatypes in the producer script itself.
Used from_json to convert json data from kafka to proper schema based data in spark.
Our data is stored in value column in dataframe in spark streaming.

5.
Handling Real-Time Data with Spark Streaming:
Processing data in real-time while maintaining low latency was a critical requirement.
Solution: Spark Streaming with Kafka integration was used to process data in micro-batches. Watermarking was applied to 
handle late data arrivals.
  
6.
Feature Engineering in Streaming Context:
Challenge- Implementing complex feature engineering like transaction frequency and average transaction amount in a streaming context.
Solution- Window functions and aggregations were used to calculate features based on a time window, ensuring that features  were calculated correctly and efficiently.
  
7. Model Integration:
Challenge: Integrating a pre-trained machine learning model into a Spark Streaming job.
Solution: The model was serialized using Joblib and loaded into the Spark job for real-time predictions.

9. Data Storage in Cassandra
Challenge: Writing high-frequency predictions to Cassandra while ensuring data consistency and avoiding bottlenecks.
Solution: Cassandra’s distributed nature and high write throughput made it suitable for storing real-time predictions.
