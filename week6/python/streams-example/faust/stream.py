import faust
from taxi_rides import TaxiRide


app = faust.App('adrian.stream.v2', broker='kafka://localhost:9092')
topic = app.topic('adrian.green_taxi_ride.json', value_type=TaxiRide)


@app.agent(topic)
async def start_reading(records):
    async for record in records:
        print(record)


if __name__ == '__main__':
    app.main()
