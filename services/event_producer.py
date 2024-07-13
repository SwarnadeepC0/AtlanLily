import json as json
import os

from kafka import KafkaProducer

class EventProducer:
    # Set up Kafka producer
    def __init__(self):
        # Kafka Producer
        kafka_host=os.getenv('KAFKA_HOST')
        kafka_port=os.getenv('KAFKA_PORT')
        self.producer = KafkaProducer(
            bootstrap_servers=[str(kafka_host)+":"+str(kafka_port)],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send(self, event, event_name):
        self.producer.send(event_name,event)
        print("sending "+str(event))

    def close(self):
        self.producer.flush()
        self.producer.close()
