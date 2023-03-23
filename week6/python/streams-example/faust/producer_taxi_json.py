import gzip
import csv
import pathlib
from json import dumps
from kafka import KafkaProducer
from time import sleep

file_path = pathlib.Path(__file__).parents[2].resolve() / "resources" / "green_tripdata_2019-01.csv.gz" 

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

with gzip.open(file_path, mode="rt", newline="") as file:
    csvreader = csv.reader(file)
    header = next(csvreader)
    for row in csvreader:
        key = {"vendorId": int(row[0])}
        value = {"vendorId": int(row[0]), "pulocationId": int(row[5]), "passenger_count": float(row[7])}
        producer.send('adrian.green_taxi_ride.json', value=value, key=key)
        print("producing")
        sleep(1)