import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import joblib


df = pd.read_csv('kafka_sensor_data.csv')


X = df[['temperature', 'pressure', 'vibration']]
y = df['label']


X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)


model = LogisticRegression()
model.fit(X_train, y_train)


y_pred = model.predict(X_test)
print("\nClassification Report:")
print(classification_report(y_test, y_pred))


joblib.dump(model, './logistic_model.pkl')
print("Model saved as 'logistic_model.pkl'")