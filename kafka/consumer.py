import pandas as pd
from kafka import KafkaConsumer
import json
import os
import boto3
from datetime import datetime



s3 = boto3.client('s3')
bucket_name = 'dataforge-bronze-asad'


output_path = "data/processed/bronze/orders.csv"


consumers = KafkaConsumer(
    'orders-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

print("Listening to Kafka-my-buddy...")

data_list = []

for message in consumers:
    data = message.value
    print("Received:", data)

    data_list.append(data)

#Saving every 50 records batching..
    if len(data_list) >= 50:
        df = pd.DataFrame(data_list)
        df.drop_duplicates(inplace=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"bronze/orders_{timestamp}.csv"

        local_file = f"/tmp/{timestamp}.csv"
        df.to_csv(local_file, index=False)

        s3.upload_file(local_file, bucket_name, file_name)
        print(f"Uploaded to S3: {file_name}")

        data_list = [] #reset

