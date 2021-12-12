# from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode
# from pyspark.sql.functions import split
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
'''
To run, please make sure you have the appropriate Spark and Scala version
- For my packages: I have Scala version 2.12 and Spark version 3.2.0 
- This is important as we need to configure the appropriate version

Then please run the following command:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 spark_connector.py
'''
spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

 # Use this code, so that when you run spark-submit
spark.sparkContext.setLogLevel("WARN")

inputDF = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "Twitter_Stream_Cleaned") \
  .load()


wordDF = inputDF.select('value', 'timestamp').withColumn('word', f.explode(f.split(f.col('value'), ' ')))\
    .withWatermark("timestamp", "1 seconds")\
    .groupBy('timestamp','word')\
    .count()\

concatDF = wordDF.withColumnRenamed('count', 'counter')

concatDF2 = concatDF.withColumn('value', f.concat(concatDF.word, concatDF.counter))

outputDf = concatDF2\
    .writeStream \
    .format("kafka").option("kafka.bootstrap.servers", "localhost:9092")\
    .option("checkpointLocation", "checkpoint_kafka/") \
    .option("topic", "WordCount") \
    .start()\
    .awaitTermination()





