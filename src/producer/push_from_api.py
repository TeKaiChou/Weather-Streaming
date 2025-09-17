# from kafka import KafkaProducer
# import time
# from weather_simulator import stream

# class Producer:
#     def __init__(self, bootstrap_servers, topic):
#         self.producer = KafkaProducer(
#             bootstrap_servers=bootstrap_servers,
#             value_serializer=lambda v: v.encode('utf-8')
#         )
#         self.topic = topic

#     def send_message(self, message):
#         try:
#             self.producer.send(self.topic, message)
#             self.producer.flush()
#             print(f"Message sent to topic {self.topic}: {message}")
#         except Exception as e:
#             print(f"Error sending message: {e}")

#     def close(self):
#         self.producer.close()



# if __name__ == "__main__":
#     # Example usage
#     bootstrap_servers = 'localhost:9092' 
#     topic = 'weather.raw'
#     producer = Producer(bootstrap_servers, topic)

#     for message in stream(loop_delay=0.5):
#         producer.send_message(message)
#         time.sleep(1)
    
#     producer.close()

#     # # Sending test messages
#     # for i in range(10):
#     #     message = {'number': i, 'temp': 20 + i, 'humidity': 30 + i}  # Example message
#     #     producer.send_message(message)
#     #     time.sleep(1)

#     # producer.close()


import os
import time 
import json
import requests
from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BROKER","localhost:9092")   # host use 9092
TOPIC  = os.getenv("RAW_TOPIC","weather.raw.v1")
API    = os.getenv("WEATHER_API","http://localhost:9001/weather")

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def main():
    while True:
        msg = requests.get(API).json()
        producer.send(TOPIC, msg)
        producer.flush() # ensure send
        print(f"Sent to {TOPIC}: {msg}")
        time.sleep(1)

if __name__ == "__main__":
    main()