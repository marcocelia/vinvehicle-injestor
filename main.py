import os
import pandas as pd
import json
from kafka import KafkaProducer, KafkaConsumer


def vin_vehicle_serializer(row):
    return json.dumps(row.to_json()).encode('utf-8')


if __name__ == '__main__':
    topic = 'VinVehicle'
    data_raw = pd.read_csv(os.path.join(os.getcwd(), '..', 'dataset', 'dati_centraline.csv'))
    data = data_raw[data_raw['Position.speed'].notna()]

    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=vin_vehicle_serializer)

    futures = []
    for i in range(0, 10):
        futures.append(producer.send(topic, data.iloc[i]))

    for future in futures:
        future.get(timeout=60)

    print(f"Successfully sent {len(futures)} row")

    # consumer = KafkaConsumer(
    #     topic,
    #     bootstrap_servers='localhost:9092',
    #     value_deserializer=lambda b: json.loads(b),
    #     auto_offset_reset='earliest'
    # )
    # for msg in consumer:
    #     print(msg.value)
