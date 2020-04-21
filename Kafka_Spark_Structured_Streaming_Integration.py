#Just Download the zookeeper,kafka, spark and nifi and below are the command for the running kafka,zookeeper,topic creation on kafka
# To run Zookeeper
# C:\kafka_2.11-2.2.0\kafka_2.11-2.2.0\bin\windows\zookeeper-server-start  C:\kafka_2.11-2.2.0\kafka_2.11-2.2.0\config\zookeeper.properties
# To Start Kafka
# C:\kafka_2.11-2.2.0\kafka_2.11-2.2.0\bin\windows\kafka-server-start  C:\kafka_2.11-2.2.0\kafka_2.11-2.2.0\config\server.properties
# Create Topic
# C:\kafka_2.11-2.2.0\kafka_2.11-2.2.0\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
# You can list the available topics using below command
# kafka-topics.bat --list --zookeeper localhost:2181
# You can start a console producer and send some messages using below command
# kafka-console-producer.bat --broker-list localhost:9092 --topic test
# You can start a console consumer and check the messages that you sent using below command
# kafka_2.12-2.0.0\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic test --from-beginning
# go to bin folder in extracted nifi then you can run from there run-nifi command and after some time you can open the http:localhost:8080/nifi where you will see nifi UI to drag and drop the further processor

from pyspark.sql import *

spark=SparkSession \
        .builder \
        .appName("Structured Streaming Application") \
        .config("spark.jars",
                "D:\\Big_Data\\Nifi\\jar_files\\kafka-clients-2.0.0.jar,"
                "D:\\Big_Data\\Nifi\\jar_files\\spark-sql-kafka-0-10_2.11-2.4.0.jar")\
        .config("spark.executor.extraClassPath",
                "D:\\Big_Data\\Nifi\\jar_files\\kafka-clients-2.0.0.jar,"
                "D:\\Big_Data\\Nifi\\jar_files\\spark-sql-kafka-0-10_2.11-2.4.0.jar")\
        .config("spark.executor.extraLibrary",
                "D:\\Big_Data\\Nifi\\jar_files\\kafka-clients-2.0.0.jar,"
                "D:\\Big_Data\\Nifi\\jar_files\\spark-sql-kafka-0-10_2.11-2.4.0.jar")\
        .config("spark.driver.extraClassPath",
                "D:\\Big_Data\\Nifi\\jar_files\\kafka-clients-2.0.0.jar,"
                "D:\\Big_Data\\Nifi\\jar_files\\spark-sql-kafka-0-10_2.11-2.4.0.jar")\
        .getOrCreate()
topic="covid_tweets"
df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers","localhost:9092")\
        .option("subscribe",topic).load()

    #Cast the Kafka Stream into the json format
json_df = df.selectExpr("CAST(value AS STRING)", "timestamp")

tweets_filtered = json_df.filter(
         df['value'].contains("#corona") | df['value'].contains("#coronavirus")
        )

#Write The Stream on console as of now.

corona_tweets = json_df.writeStream\
        .trigger(processingTime="1 second")\
        .outputMode("append")\
        .option("truncate","false")


corona_tweets\
        .format("console")\
        .start()\
        .awaitTermination()

