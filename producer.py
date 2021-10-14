from kafka import KafkaProducer
from json import dumps
import time


TOPIC_NAME = 'items'
KAFKA_SERVER = 'localhost:29092'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

for x in range(1000):
    data = {"n": x}
    producer.send(TOPIC_NAME, value=data)
    print(f"Sent data ...{x}")
    time.sleep(5)
