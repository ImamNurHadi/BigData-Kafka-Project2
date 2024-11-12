from kafka import KafkaProducer
import json
import time
import random
import csv

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('iot_telemetry_data.csv', 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.send('baca-tel', row) 
        print("Mengirim data:", row) 
        time.sleep(random.uniform(0.5, 2))

producer.close()
print("Data dari iot_telemntary_data.csv berhasil dikirim ke Kafka.")
