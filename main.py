import os
import pandas as pd
from kafka import KafkaProducer


def vin_vehicle_serializer(row):
    return ','.join([str(x) for x in row.to_list()]).encode('utf-8')


if __name__ == '__main__':
    topic = 'VinVehicle'
    data_raw = pd.read_csv(os.path.join(os.getcwd(), '..', 'dataset', 'dati_centraline.csv'))
    data = data_raw[data_raw['Position.speed'].notna()]

    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=vin_vehicle_serializer)

    futures = []
    for i in range(0, len(data)):
        print(f"Producing row {i}")
        futures.append(producer.send(topic, data.iloc[i]))

    for future in futures:
        future.get(timeout=60)

    print(f"Successfully sent {len(futures)} row")

