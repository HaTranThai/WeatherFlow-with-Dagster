from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
)
import logging

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'Current_data') \
            .option('startingOffsets', 'latest') \
            .option("failOnDataLoss", "false") \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    
    schema = StructType([
        StructField("coord", StructType([
            StructField("lon", DoubleType(), True),
            StructField("lat", DoubleType(), True)
        ]), True),
        StructField("weather", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("main", StringType(), True),
            StructField("description", StringType(), True),
            StructField("icon", StringType(), True)
        ])), True),
        StructField("base", StringType(), True),
        StructField("main", StructType([
            StructField("temp", DoubleType(), True),
            StructField("feels_like", DoubleType(), True),
            StructField("temp_min", DoubleType(), True),
            StructField("temp_max", DoubleType(), True),
            StructField("pressure", IntegerType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("sea_level", IntegerType(), True),
            StructField("grnd_level", IntegerType(), True)
        ]), True),
        StructField("visibility", IntegerType(), True),
        StructField("wind", StructType([
            StructField("speed", DoubleType(), True),
            StructField("deg", IntegerType(), True),
            StructField("gust", DoubleType(), True)
        ]), True),
        StructField("clouds", StructType([
            StructField("all", IntegerType(), True)
        ]), True),
        StructField("dt", IntegerType(), True),
        StructField("sys", StructType([
            StructField("country", StringType(), True),
            StructField("sunrise", IntegerType(), True),
            StructField("sunset", IntegerType(), True)
        ]), True),
        StructField("timezone", IntegerType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("cod", IntegerType(), True)
    ])


    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

spark_conn = create_spark_connection()

if spark_conn is None:
    print("ERROR: SparkSession không được khởi tạo.")
else:
    spark_df = connect_to_kafka(spark_conn)
    if spark_df is None:
        print("ERROR: Kafka DataFrame không được tạo.")
    else:
        selection_df = create_selection_df_from_kafka(spark_df)
        query = selection_df.limit(10).writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        query.awaitTermination()

