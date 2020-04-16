import json
import findspark
findspark.init()
from pyspark.sql import SparkSession
from kafka import KafkaProducer

TOPIC_NAME = 'events'


def connect_kafka_producer():
    """ Connects to kafka server and creates a producer instance.

    :return: Kafka producer instance
    """
    _producer = None
    kafka_url = 'localhost:9092'
    try:
        _producer = KafkaProducer(bootstrap_servers=kafka_url,
                                  value_serializer=lambda value: json.dumps(value).encode())
    except Exception as ex:
        print('Exception while connecting to Kafka..')
        print(str(ex))
    finally:
        return _producer


def read_json(spark, path):
    """ Read the json data from the given path and call the 'send_to_kafka' function on each partition.

    :param spark: SparkSession
    :param path: str
    """
    input_df = spark.read.json(path, multiLine=True)
    input_df.foreachPartition(send_to_kafka)


def send_to_kafka(rows):
    """ Send the data through Kafka producer to a topic.

    :param rows: itertools.chain iterator
    """
    producer = connect_kafka_producer()
    for row in rows:
        print(row.asDict())
        producer.send(TOPIC_NAME, value=row.asDict())
        producer.flush()


if __name__ == '__main__':
    # Path for input data
    file_path = 'data/MOCK_DATA.json'
    # Create SparkSession
    spark = SparkSession.builder.appName("SparkKafka").getOrCreate()

    read_json(spark, file_path)
