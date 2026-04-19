from kafka import KafkaProducer
import time
import pandas as pd
import json


df = pd.read_csv("data/raw/olist_orders_dataset.csv")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for _, row in df.iterrows():
    data = row.to_dict()
    producer.send('orders-topic', value=data)
    print("Sent:", data)
    time.sleep(1) #Important

producer.flush() #Making sure all messages are sent before exiting
