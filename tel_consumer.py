from kafka import KafkaConsumer
import json
import time
import os
from datetime import datetime
import logging

consumer = KafkaConsumer(
    'baca-tel',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest', 
    enable_auto_commit=True,  
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

batch_size = 1000 
time_window = 300 
batch_data = []
batch_count = 1
start_time = time.time()

batch_directory = "batches/"
os.makedirs(batch_directory, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info("Kafka Consumer mulai menerima data...")

def save_batch(batch_data, batch_count):
    if batch_data:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = os.path.join(batch_directory, f'batch_{batch_count}_{timestamp}.json')
        with open(filename, 'w') as file:
            json.dump(batch_data, file)
        print(f"Batch {batch_count} disimpan ke {filename}")

try:
    for message in consumer:
        batch_data.append(message.value)
        print(f"Pesan diterima: {message.value}") 

        current_time = time.time()
        if len(batch_data) >= batch_size or (current_time - start_time) >= time_window:
            save_batch(batch_data, batch_count)  
            batch_data = [] 
            batch_count += 1 
            start_time = current_time  

except KeyboardInterrupt:
    print("Proses consumer dihentikan oleh pengguna.")
finally:
    if batch_data:
        save_batch(batch_data, batch_count)
    
    # Tutup consumer
    consumer.close()
    print("Kafka Consumer selesai menerima data.")
