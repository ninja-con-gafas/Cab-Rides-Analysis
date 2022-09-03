from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum as pyspark_sum

spark = SparkSession \
    .builder \
    .appName('aggregate_bookings_data') \
    .enableHiveSupport() \
    .getOrCreate()

bookings = spark.sql("SELECT * FROM TBL_BOOKINGS")

bookings_aggregated = bookings.withColumn("trip_date", to_date(col("pickup_timestamp"), "yyyy-MM-dd")) \
    .groupBy(col("trip_date")) \
    .agg(
        count("booking_id").alias("number_of_trips"),
        pyspark_sum("trip_fare").alias("total_trip_amount"),
        pyspark_sum("tip_amount").alias("total_tip_amount"),
        pyspark_sum("passenger_count").alias("total_passenger_counts")
        ) \
    .orderBy("trip_date")

bookings_aggregated.createOrReplaceTempView("VW_AGGREGATED_BOOKINGS")
spark.sql("CREATE TABLE IF NOT EXISTS TBL_AGGREGATED_BOOKINGS \
            AS SELECT * FROM VW_AGGREGATED_BOOKINGS")

bookings_aggregated \
    .write \
    .csv("/user/livy/aggregate_batch_data", header="false")
