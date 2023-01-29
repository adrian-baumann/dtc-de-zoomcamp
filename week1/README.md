# Week 1 
Docker, SQL and Terraform

## Homework
### Part A
- Used [notebook](./upload-data.ipynb) from course and modified to upload data into PostgreSQL DB
- Used pgcli to solve SQL questions about taxi data
- later used [ingest_data.py](./ingest_data.py) and pgadmin in docker (see notes)
- Queries used for Questions 3-6 found in [`homework-queries.sql`](./homework-queries.sql)
### Part B
- tried terraform locally and afterwards setup in gcp virtual machine
- posted output of `terraform apply` in homework google form

------
## Notes

Run `ingest_data.py` with Docker
1. To create docker containers in network run: 
    ```bash
    docker-compose up
    ```

2. Run
    ```bash
    docker build -t taxi_ingest:v001 ./week1/
    ```
2. Run with appropriate URL and table name
    ```bash
    URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    NAME="yellow_taxi_data"

    docker run -it \
    --network=week1_default \
    taxi_ingest:v001 \
        --user=root \
        --password=root \
        --host=pgdatabase \
        --port=5432 \
        --db=ny_taxi \
        --table_name=${NAME} \
        --url=${URL}
    ```

