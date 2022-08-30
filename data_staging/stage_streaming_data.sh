#!/usr/bin/env bash

hdfs dfs -rm -r /user/livy/click_stream_data

hdfs dfs -mkdir -p /user/livy/click_stream_data

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 data_staging/stage_streaming_data.py
