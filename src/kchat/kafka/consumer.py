# src/kchat/kafka/consumer.py

from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'topic1',
   # configs = {},
    bootstrap_servers = ['localhost:9092'],
    value_deserializer = lambda x: loads(x.decode('utf-8')),    
    consumer_timeout_ms = 5000
)

print('[START] get consumer')

for  message in consumer:
    print(message)

print('[END] get consumer')
