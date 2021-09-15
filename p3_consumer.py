from kafka import KafkaConsumer
from json import loads
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

spark = SparkSession.builder.appName('p3').getOrCreate()

KAFKA_TOPIC_NAME_CONS = 'p3-topic'
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
KAFKA_TOPIC_GROUP_NAME_CONS = 'my-group'

if __name__ == "__main__":

    schema = StructType([
        StructField('order_id', IntegerType(), True),
        StructField('customer_id', IntegerType(), True),
        StructField('customer_name', StringType(), True),
        StructField('country', StringType(), True),
        StructField('city', StringType(), True),
        StructField('product_id', IntegerType(), True),
        StructField('product_name', StringType(), True),
        StructField('product_category', StringType(), True),
        StructField('payment_type', StringType(), True),
        StructField('qty', IntegerType(), True),
        StructField('price', FloatType(), True),
        StructField('order_datetime', StringType(), True),
        StructField('ecommerce_website_name', StringType(), True),
        StructField('payment_txn_id', StringType(), True),
        StructField('payment_txn_status', StringType(), True),
        StructField('failure_reason', StringType(), True)
    ])

    df = spark.createDataFrame([], schema)
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME_CONS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=KAFKA_TOPIC_GROUP_NAME_CONS,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    for message in consumer:
        x = [message.value]
        df1 = spark.createDataFrame(x, schema)
        df = df.union(df1)
        df.printSchema()
        df.show(truncate=False)