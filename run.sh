#!/usr/bin/env bash

sh data_staging/setup_mysql_connector.sh

sh data_staging/stage_streaming_data.sh

sh data_staging/stage_batch_data.sh

hdfs dfs -mkdir -p {/user/livy/feeds_path/click_stream_data,/user/livy/feeds_path/bookings}
hdfs dfs -cp /user/livy/click_stream_data/*.csv /user/livy/feeds_path/click_stream_data
hdfs dfs -cp /user/livy/bookings/part* /user/livy/feeds_path/bookings

hive -f data_sourcing/ingest_streaming_data.sql
hive -f data_sourcing/ingest_batch_data.sql

spark-submit aggregate_batch_data.py
