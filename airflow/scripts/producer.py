from kafka import KafkaProducer
import time
import pandas as pd
import json


df = pd.read_csv("/opt/airflow/data/raw/olist_orders_dataset.csv")

producer = KafkaProducer(
    bootstrap_servers='host.docker.internal:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for _, row in df.iterrows():
    data = row.to_dict()
    producer.send('orders-topic', value=data)

producer.flush() #Making sure all messages are sent before exiting
