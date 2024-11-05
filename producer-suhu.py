import random
import time
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_ids = ['S1', 'S2', 'S3']

def generate_suhu(sensor_id):
    return {
        "sensor_id": sensor_id,
        "suhu": random.uniform(20, 100)  # Suhu random interval 20 - 100 derajat
    }

try:
    for _ in range(100):  # Loop untuk mengirim 100 data
        for sensor_id in sensor_ids:
            suhu_data = generate_suhu(sensor_id)

            producer.send('sensor-suhu', value=suhu_data)
            print(f'Data {suhu_data} dikirim')
        time.sleep(1)  # Mengirim data setiap detik
except KeyboardInterrupt:
    print("Producer berhenti")
finally:
    producer.close()
