from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

kafka_topic_name = 'p3-topic'
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark structured streaming") \
        .master("local") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

    print("Printing Schema of orders_df: ")
    df.printSchema()