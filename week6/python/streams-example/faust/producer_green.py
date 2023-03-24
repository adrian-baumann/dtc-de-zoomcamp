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
        value = {"vendorID":int(row[0]), "lpep_pickup_datetime":str(row[1]),	"lpep_dropoff_datetime":str(row[2]),	"store_and_fwd_flag":str(row[3]),	"RatecodeID":int(row[4]),	"PULocationID":str(row[5]),	"DOLocationID":str(row[6]),	"passenger_count":int(row[7]),	"trip_distance":float(row[8]),	"fare_amount":float(row[9]),	"extra":float(row[10]),	"mta_tax":float(row[11]),	"tip_amount":float(row[12]),	"tolls_amount":float(row[13]),	"ehail_fee":str(row[14]),	"improvement_surcharge":float(row[15]),	"total_amount":float(row[16]),	"payment_type":int(row[17]),	"trip_type":int(row[18]),	"congestion_surcharge":str(row[19])}
        producer.send('adrian.green_taxi_rides.json', value=value, key=key)
        print("producing")
        sleep(1)



