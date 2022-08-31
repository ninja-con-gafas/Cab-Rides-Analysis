CREATE TABLE IF NOT EXISTS TBL_BOOKINGS
(
    BOOKING_ID                  STRING,
    CUSTOMER_ID                 INT,
    DRIVER_ID                   INT,
    CUSTOMER_APP_VERSION        STRING,
    CUSTOMER_PHONE_OS_VERSION   STRING,
    PICKUP_LAT                  FLOAT,
    PICKUP_LON                  FLOAT,
    DROP_LAT                    FLOAT,
    DROP_LON                    FLOAT,
    PICKUP_TIMESTAMP            TIMESTAMP,
    DROP_TIMESTAMP              TIMESTAMP,
    TRIP_FARE                   FLOAT,
    TIP_AMOUNT                  FLOAT,
    CURRENCY_CODE               STRING,
    CAB_COLOR                   STRING,
    CAB_REGISTRATION_NO         STRING,
    CUSTOMER_RATING_BY_DRIVER   INT,
    RATING_BY_CUSTOMER          INT,
    PASSENGER_COUNT             INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH "hdfs:///user/livy/feeds_path/bookings"
OVERWRITE INTO TABLE TBL_BOOKINGS;
