#kafka_streaming_json.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

kafka_topic_name = "demo1"
kafka_bootstrap_servers = 'localhost:9092'
card_topic_name = 'card1'

if __name__ == "__main__":
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from p3-topic
    orders_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print('*****Streaming schema*****')
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)")

    schema = StructType() \
        .add('order_id', IntegerType()) \
        .add('customer_id', IntegerType()) \
        .add('customer_name', StringType()) \
        .add('country', StringType()) \
        .add('city', StringType()) \
        .add('product_id', IntegerType()) \
        .add('product_name', StringType()) \
        .add('product_category', StringType()) \
        .add('payment_type', StringType()) \
        .add('qty', IntegerType()) \
        .add('price', IntegerType()) \
        .add('order_datetime', TimestampType()) \
        .add('ecommerce_website_name', StringType()) \
        .add('payment_txn_id', StringType()) \
        .add('payment_txn_status', StringType()) \
        .add('failure_reason', StringType())

    orders_df2 = orders_df1 \
        .select(from_json(col("value"), schema).alias("data")).select('data.*')

    print('*****DataFrame Schema*****')
    orders_df2.printSchema()

    # print("==========Normal DataFrame==========")
    # od_df = orders_df2.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .start()

    od_card = orders_df2.filter("payment_type=='Card'")

    od_card1 = od_card.withColumn("key", lit(100)) \
                        .withColumn("value", concat(lit("{'order_id': '"), col('order_id').cast("string"),
                                                    lit("', 'customer_id': '"), col('customer_id').cast("string"),
                                                    lit("', 'customer_name': '"), col('customer_name'),
                                                    lit("', 'country': '"), col('country'),
                                                    lit("', 'city': '"), col('city'),
                                                    lit("', 'product_id': '"), col('product_id').cast("string"),
                                                    lit("', 'product_name': '"), col('product_name'),
                                                    lit("', 'product_category': '"), col('product_category'),
                                                    lit("', 'payment_type': '"), col('payment_type'),
                                                    lit("', 'qty': '"), col('qty').cast("string"),
                                                    lit("', 'price': '"), col('price').cast("string"),
                                                    lit("', 'order_datetime': '"), col('order_datetime'),
                                                    lit("', 'ecommerce_website_name': '"), col('ecommerce_website_name'),
                                                    lit("', 'payment_txn_id': '"), col('payment_txn_id'),
                                                    lit("', 'payment_txn_status': '"), col('payment_txn_status'),
                                                    lit("', 'failure_reason': '"), col('failure_reason'), lit("'}")
                                                    ))

    od_card1.printSchema()

    trans_detail_write_stream_1 = od_card1 \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", card_topic_name) \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("checkpointLocation", "file:///home/hdoop/tmp/py_checkpoint") \
        .start()

    trans_detail_write_stream_1.awaitTermination()

    # trans_detail_write_stream = od_card1 \
    #     .writeStream \
    #     .trigger(processingTime='1 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false")\
    #     .format("console") \
    #     .start()

    # trans_detail_write_stream.awaitTermination()

    # od_parquet = orders_df2.withWatermark("order_datetime", "2 seconds") \
    #     .groupBy("city", "payment_type", window("order_datetime", "2 seconds")) \
    #     .agg(sum(col('qty')*col('price')).alias('total_amount'), \
    #     count('order_id').alias('total_orders')) \
    #     .select('city', 'payment_type', 'total_amount', 'total_orders')

    # od_parquet1 = od_parquet.writeStream \
    #     .format('parquet') \
    #     .outputMode('append') \
    #     .option('path', 'p3/output') \
    #     .option("checkpointLocation", "file:///home/hdoop/tmp/parquet_checkpoint") \
    #     .start()

    # od_df = od_df1.writeStream \
    #     .trigger(processingTime='20 seconds')\
    #     .outputMode("update") \
    #     .format("console") \
    #     .start()

    # od_parquet1.awaitTermination()
    # od_df.awaitTermination()