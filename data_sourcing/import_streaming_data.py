from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col

spark = SparkSession \
    .builder \
    .appName("stage_click_stream_data") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

schema = StructType(
    [
        StructField(name="customer_id", dataType=StringType(), nullable=True),
        StructField(name="app_version", dataType=StringType(), nullable=True),
        StructField(name="OS_version", dataType=StringType(), nullable=True),
        StructField(name="lat", dataType=StringType(), nullable=True),
        StructField(name="lon", dataType=StringType(), nullable=True),
        StructField(name="page_id", dataType=StringType(), nullable=True),
        StructField(name="button_id", dataType=StringType(), nullable=True),
        StructField(name="is_button_click", dataType=StringType(), nullable=True),
        StructField(name="is_page_view", dataType=StringType(), nullable=True),
        StructField(name="is_scroll_up", dataType=StringType(), nullable=True),
        StructField(name="is_scroll_down", dataType=StringType(), nullable=True),
        StructField(name="timestamp\n", dataType=StringType(), nullable=True)
    ]
)

click_stream_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("subscribe", "de-capstone3") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("click_stream_data")) \
    .select("click_stream_data.*") \
    .withColumnRenamed("OS_version", "os_version") \
    .withColumnRenamed("timestamp\n", "timestamp")

write_click_stream_data = click_stream_data \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("truncate", "false") \
    .option("path", "/user/livy/click_stream_data") \
    .option("checkpointLocation", "hdfs:///user/livy/checkpoints/click_stream_data") \
    .trigger(once=True) \
    .start()

write_click_stream_data.awaitTermination()
