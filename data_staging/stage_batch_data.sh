#!/usr/bin/env bash

hdfs dfs -rm -r /user/livy/bookings

sqoop import \
--connect jdbc:mysql://upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com/testdatabase \
--table bookings \
--username student --password STUDENT123 \
--target-dir /user/livy/bookings \
-m 1
