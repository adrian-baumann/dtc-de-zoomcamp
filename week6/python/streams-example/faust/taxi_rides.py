import faust


class TaxiRide(faust.Record, validation=True):
    vendorId: str
    pulocationId: int
    passenger_count: int