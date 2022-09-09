#!/usr/bin/env bash

echo ">>> Setup Apache Hadoop Distributed File System (HDFS) directories to import click stream data"

hdfs dfs -rm -r /user/livy
hdfs dfs -mkdir -p /user/livy/click_stream_data

echo ">>> Run the Apache Spark Structured Streaming application to read data from Apache Kafka topic and store as CSV files in HDFS"

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 data_sourcing/import_streaming_data.py

echo ">>> Download and install MySQL connector in Amazon Elastic Map Reduce (EMR) master node"

wget "https://de-mysql-connector.s3.amazonaws.com/mysql-connector-java-8.0.25.tar.gz"
sudo rm /usr/lib/sqoop/lib/mysql-connector-java-8.0.25.jar
sudo tar -xvf mysql-connector-java-8.0.25.tar.gz --strip-components=1 --directory /usr/lib/sqoop/lib/ mysql-connector-java-8.0.25/mysql-connector-java-8.0.25.jar


echo ">>> Run the Apache Sqoop job to import bookings data from Amazon Relational Database Service (RDS) into HDFS"

sqoop import \
--connect jdbc:mysql://upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com/testdatabase \
--table bookings \
--username student --password STUDENT123 \
--target-dir /user/livy/bookings \
-m 1

echo ">>> Setup feeds path and copy data into the feeds path for ingestion of the imported data into Apache Hive"

hdfs dfs -mkdir -p {/user/livy/feeds_path/click_stream_data,/user/livy/feeds_path/bookings}
hdfs dfs -cp /user/livy/click_stream_data/*.csv /user/livy/feeds_path/click_stream_data
hdfs dfs -cp /user/livy/bookings/part* /user/livy/feeds_path/bookings

echo  ">>> Ingest the click stream data and bookings data into Hive tables"

hive -f data_sourcing/ingest_streaming_data.sql
hive -f data_sourcing/ingest_batch_data.sql

echo ">>> Run Apache Spark application to aggregate bookings data, load the aggregated data into a Hive table and store as CSV files in HDFS"

spark-submit data_transformation/aggregate_batch_data.py
