# import os
# import findspark
# findspark.init('/usr/hdp/3.0.1.0-187/spark2')


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from config import CONFIGS, TOPIC_NAME

print('*'*100)
sc = SparkContext(appName="PythonSparkStreamingKafka").getOrCreate()
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 60)

kafkaStream = KafkaUtils.createStream(
    ssc=ssc,
    zkQuorum=f"{CONFIGS['KAFKA_ADDRESS']}:{CONFIGS['KAFKA_PORT']}",
    groupId=None,
    topics={TOPIC_NAME: 1}
)

lines = kafkaStream.map(lambda x: x[1])
ssc.start()
lines.pprint()

ssc.awaitTermination()
print('#'*100)
# spark = SparkSession.builder.master("local[*]").appName("Datacleaning").getOrCreate()
#
#
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667") \
#   .option("subscribe", TOPIC_NAME) \
#   .load().s
# df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
#
#
# df.show()