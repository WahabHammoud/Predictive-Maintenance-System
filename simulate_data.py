from kafka import KafkaProducer
import json
import random
import time


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def generate_sensor_data():
    return {
        "temperature": round(random.uniform(60, 100), 2),
        "pressure": round(random.uniform(1.0, 5.0), 2),
        "vibration": round(random.uniform(0.1, 1.0), 2),
        "label": random.choice([0, 1])
    }

try:
    while True:
        data = generate_sensor_data()
        producer.send("sensor-data", data)
        print(f"Sent: {data}")
        time.sleep(1)
except KeyboardInterrupt:
    print("Data generation stopped.")
