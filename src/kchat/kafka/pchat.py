# src/kchat/kafka/pchat

from kafka import KafkaProducer
from json import dumps
import time

p = KafkaProducer(
    # TODO

)

print("Beginning CHAT - SENDER")
print("Type in message to send... (type 'exit' to finish)")

while True:
    msg = input("YOU: ")
    if msg == 'exit':
        break
    data = {'message': msg, 'time': time.time()}
   
    # TODO
 
print("Ending CHAT...")
