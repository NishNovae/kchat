# src/kchat/kafka/consumer.py

from kafka import KafkaConsumer, TopicPartition
from json import loads
import os

OFFSET_FILE = 'consumer_offset.txt'

def save_offset(offset):
    with open(OFFSET_FILE, 'w') as file:
        file.write(str(offset))

def read_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, 'r') as file:
            return int(file.read().strip())
    return None

saved_offset = read_offset()

consumer = KafkaConsumer(
#    'topic1',
   # configs = {},
    bootstrap_servers = ['localhost:9092'],
    value_deserializer = lambda x: loads(x.decode('utf-8')),    
    consumer_timeout_ms = 5000,
    auto_offset_reset='earliest' if read_offset() is None else 'none',
    group_id = 'fbi',
    enable_auto_commit=False
)

print('[START] get consumer')

p = TopicPartition('topic1', 0)
consumer.assign([p])

if saved_offset is not None:
#    p = TopicPartition('topic1', 0)
#    consumer.assign([p])    # assign partition
    consumer.seek(p, saved_offset)
else:
    consumer.seek_to_beginning(p)

for  m in consumer:
    print(f"offset={m.offset}, value={m.value}")
    save_offset(m.offset +1)
#    print(f"topic={m.topic}, partition={m.partition}, offset={m.offset}, value={m.value}, timestamp={m.timestamp}")

print('[END] get consumer')
