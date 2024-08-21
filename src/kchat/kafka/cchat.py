# src/kchat/kafka/cchat.py

from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'chatting',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='chat_group',
    value_deserializer= lambda x: loads(x.decode('utf-8'))
)

print("Beginning CHAT - Receiving Messages")
print("Standby...")

try:
    for m in consumer:
        data = m.value
        print(f"[FRIEND]: {data['message']}")    
except KeyboardInterrupt:
    print("Ending CHAT...")
finally:
    consumer.close()



