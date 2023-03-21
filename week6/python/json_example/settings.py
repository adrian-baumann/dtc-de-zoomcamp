import pathlib

INPUT_DATA_PATH = str(pathlib.Path().absolute() / "python" / "resources" / "rides.csv")
BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "rides_json"
