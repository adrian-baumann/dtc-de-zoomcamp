import faust
from taxi_rides import TaxiRide
from faust import current_event

app = faust.App('adrian.stream.v3', broker='kafka://localhost:9092', consumer_auto_offset_reset="earliest")
topic = app.topic('adrian.green_taxi_ride.json', value_type=TaxiRide)

mult_ppl_rides = app.topic('adrian.green_taxi_rides.many')
sngl_ppl_rides = app.topic('adrian.green_taxi_rides.single')


@app.agent(topic)
async def process(stream):
    async for event in stream:
        if event.passenger_count >= 2:
            await current_event().forward(mult_ppl_rides)
        else:
            await current_event().forward(sngl_ppl_rides)

if __name__ == '__main__':
    app.main()
