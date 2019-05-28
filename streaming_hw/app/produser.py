import json
from kafka import KafkaProducer
from threading import Thread
from config import CONFIGS, TOPIC_NAME
from helpers import generate_booking_event


class Producer(Thread):
    def __init__(self, thread_name: str, number_of_messages: int, topic_name: str):
        Thread.__init__(self)
        self.name = thread_name
        self.number_of_messages = number_of_messages
        self.topic = topic_name
        self.producer = KafkaProducer(
            bootstrap_servers=f"{CONFIGS['KAFKA_ADDRESS']}:{CONFIGS['KAFKA_PORT']}",
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.producer.close()
        return True

    def send(self, topic, msg):
        self.producer.send(topic=topic, value=msg)

    def run(self) -> None:
        print(f'Thread {self.name} started')
        for i in range(self.number_of_messages):
            self.send(self.topic, generate_booking_event())
        self.producer.close()
        print(f'Thread {self.name} completed')
