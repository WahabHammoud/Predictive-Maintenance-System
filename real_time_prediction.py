from kafka import KafkaConsumer
import joblib
import json
import pandas as pd
from pymongo import MongoClient


model = joblib.load('./logistic_model.pkl')


client = MongoClient("mongodb://localhost:27017/")
db = client["predictive_maintenance"]
collection = db["predictions"]


consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Real-time prediction started...")


for message in consumer:
    data = message.value
    features = pd.DataFrame([data], columns=['temperature', 'pressure', 'vibration'])
    prediction = model.predict(features)[0]  # Predict the label


    data['predicted_label'] = int(prediction)


    collection.insert_one(data)
    print(f"Data: {data}, Saved to MongoDB")
