from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable
import time
from . import LOGGER


def check_if_topic_created(topic_name: str, bootstrap_servers: list) -> bool:
    while True:
        LOGGER.info('check if kafka topic created')
        try:
            consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
            topics = consumer.topics()
            break
        except NoBrokersAvailable:
            time.sleep(5)
    return topic_name in topics


def create_topics(bootstrap_servers: list, topic_names: list):
    LOGGER.info(f"{'-'*10} create topic {'-'*10}")
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topic_list = []
    for topic in topic_names:
        topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
    while True:
        try:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            break
        except Exception as e:
            LOGGER.error("Couldn't connect to Kafka broker because of %s, try again in 3 seconds", e)
            time.sleep(3)
