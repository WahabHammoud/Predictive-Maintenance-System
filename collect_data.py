from kafka import KafkaConsumer
import pandas as pd
import json


consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


records = []

print("Collecting data from Kafka...")

try:
    for message in consumer:
        records.append(message.value)
        print(f"Received: {message.value}")


        if len(records) >= 100:
            break
except KeyboardInterrupt:
    print("Stopped collecting data.")


df = pd.DataFrame(records)


df.to_csv('kafka_sensor_data.csv', index=False)
print("\nData saved to 'kafka_sensor_data.csv'")
