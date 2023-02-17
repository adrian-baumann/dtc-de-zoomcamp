# Questions

## Question 1:

### What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)
- 61,646,553

## Question 2:

### What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos
- 89.9% yellow / 10.1% green
- see [this pdf](./q2_viz.pdf)

## Question 3:

### What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false) <br>Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false). Filter records with pickup time in year 2019.
- 43,244,693

## Question 4:

### What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false) <br>Create a core model for the stg_fhv_tripdata joining with dim_zones. Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.
- 22,998,722

## Question 5:

### What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.
- January, with almost 20 million trips
- for the dashboard see [this pdf](./q5_viz.pdf)
