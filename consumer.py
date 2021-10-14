from kafka import KafkaConsumer
from json import loads


TOPIC_NAME = 'items'

consumer = KafkaConsumer(TOPIC_NAME,
                         bootstrap_servers=['localhost:29092'],
                         value_deserializer=lambda x: loads(x.decode('utf-8')))
for message in consumer:
    print(message)
