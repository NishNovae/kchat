# src/kchat/kafka/pchat

from kafka import KafkaProducer
#from json import dumps, loads
import json
import time

p = KafkaProducer(
    # 'chat',
    bootstrap_servers = ['localhost:9092'],
    value_serializer = lambda x: json.dumps(x).encode('utf-8')
)

print("Beginning CHAT - SENDER")
print("Type in message to send... (type 'exit' to finish)")

while True:
    msg = input("YOU: ")
    if msg == 'exit':
        break
    data = {'message': msg, 'time': time.time()}
    p.send('chatting', value=data)   
    p.flush()
    time.sleep(1)

end = time.time()
print("Ending CHAT...")
print("[DONE]:", end - start)

