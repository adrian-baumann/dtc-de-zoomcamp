import gzip
import csv
import pathlib
from json import dumps
from kafka import KafkaProducer
from time import sleep

file_path = pathlib.Path(__file__).parents[2].resolve() / "resources" / "fhv_tripdata_2019-01.csv.gz" 

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

with gzip.open(file_path, mode="rt", newline="") as file:
    csvreader = csv.reader(file)
    header = next(csvreader)
    for row in csvreader:
        key = {"rideId": str(row[0]) + str(row[1])}
        value = {"dispatching_base_num":str(row[0]), "pickup_datetime":str(row[1]),	"dropOff_datetime":str(row[2]),	"PUlocationID":str(row[3]),	"DOlocationID":str(row[4]),	"SR_Flag":str(row[5]),	"Affiliated_base_number":str(row[6])}
        producer.send('adrian.fhv_rides.json', value=value, key=key)
        print("producing")
        sleep(1)