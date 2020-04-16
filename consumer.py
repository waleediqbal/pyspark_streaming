import ipaddress
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, count, window, max, min, desc, asc, countDistinct
from pyspark.sql.types import *


TOPIC_NAME = "events"
UNKNOWN_VALUE = "NA"
# Apply this schema when reading the messages from Kafka topic, using the 'from_json' sql function
SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), False),
    StructField("gender", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("date", StringType(), False),
    StructField("country", StringType(), True)
])


def execute_queries(df, epoch_id):
    """ For each micro-batch of streaming data, calculate simple measures of the data.

    :param df: Spark DataFrame
    :param epoch_id: int - unique ID of the micro-batch
    """
    print("Batch ID: {}".format(epoch_id))
    df.persist()

    print("Most represented country: ")
    df.groupBy("country").count().orderBy(desc("count")).limit(1).show()
    print("Least represented country: ")
    df.groupBy("country").count().orderBy(asc("count")).limit(1).show()
    print("Unique users in the batch: ")
    df.select(countDistinct("id")).show()
    print('_' * 30)

    df.unpersist()


@udf
def to_title(s):
    """ Capitalize the string s.

    :param s: str
    :return: str - s converted to Uppercase else UNKNOWN_VALUE if s is None
    """
    return UNKNOWN_VALUE if s is None else s.title()


@udf
def validate_gender(gender):
    """ Validate if the gender is correct, else return UNKNOWN_VALUE

    :param gender: str
    :return: str - gender if valid else UNKNOWN_VALUE
    """
    return UNKNOWN_VALUE if gender not in ["Male", "Female"] else gender


@udf
def validate_ip_address(addr):
    """ Using the python ipaddress module, validate the ip address.

    :param addr: str
    :return: str - addr if valid else UNKNOWN_VALUE
    """
    if addr is None:
        return UNKNOWN_VALUE
    try:
        ipaddress.ip_address(addr)
    except ValueError:
        return UNKNOWN_VALUE
    return addr


if __name__ == '__main__':
    # Create SparkSession
    spark = SparkSession.builder.appName("StreamingProcessing").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Subscribe to events topic from kafka using Spark structured streaming
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "events") \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)") \
        .select(from_json("value", SCHEMA).alias("value")).select("value.*")

    # Data cleaning for country, ip_address and gender columns
    df = df.withColumn("country", to_title("country"))
    df = df.withColumn("ip_address", validate_ip_address("ip_address"))
    df = df.withColumn("gender", validate_gender("gender"))

    # For each micro-batch of streaming data, execute our function 'execute_queries'
    df.writeStream.foreachBatch(execute_queries).start().awaitTermination()
