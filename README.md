# pyspark_streaming

The solution was developed and tested in the following environment.

## Environment

* Ubuntu 16.04
* Python 3.5.7
* Spark 2.4.5
  * Structured Streaming
* Kafka - release 2.4.1


## How to setup

### Kafka
Download Kafka from the link (https://kafka.apache.org/quickstart). 

Once downloaded, run the following commands:
  * tar -xzf kafka_2.12-2.4.1.tgz
  * cd kafka_2.12-2.4.1

#### Start ZooKeeper
sudo bin/zookeeper-server-start.sh config/zookeeper.properties

#### Start Kafka Server
sudo bin/kafka-server-start.sh config/server.properties

#### Create Kafka Topic
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic events


### Spark  

Download Spark (https://www.apache.org/dyn/closer.lua/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz).


Unpack the archive
* tar -xvf spark-2.4.5-bin-hadoop2.7.tgz


Run the following commands on a terminal:
  * unset PYSPARK_DRIVER_PYTHON
  * export SPARK_HOME='/{YOUR_SPARK_DIRECTORY}/spark-2.4.5-bin-hadoop2.7'

### Python Libraries
Using pip (https://pip.pypa.io/en/stable/installing/) for python3 install the following libraries:
   * pip install findspark
   * pip install kafka-python 

_findspark_ library adds pyspark to sys.path at runtime

## How to run
Clone the github repository
* git clone https://github.com/waleediqbal/pyspark_streaming.git
* cd pyspark_streaming/ 
* Run the consumer using the following command in a terminal

        spark-submit --master local[2] --conf "spark.dynamicAllocation.enabled=false" --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 consumer.py
        
* Run the producer using the following command in a terminal

        spark-submit --master local[2] --conf "spark.dynamicAllocation.enabled=false" producer.py
        