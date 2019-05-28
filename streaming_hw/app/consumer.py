from kafka import KafkaConsumer
from config import CONFIGS, TOPIC_NAME


consumer = KafkaConsumer(auto_offset_reset='earliest', bootstrap_servers=f"{CONFIGS['KAFKA_ADDRESS']}:{CONFIGS['KAFKA_PORT']}", group_id='group 1')
consumer.subscribe([TOPIC_NAME])
for msg in consumer:
    print(msg)