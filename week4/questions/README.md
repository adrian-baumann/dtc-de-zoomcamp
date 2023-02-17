# Questions
## Question 1:
### What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)
- 61,646,553

## Question 2:
### What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos
- 89.9% yellow / 10.1% green

## Question 3:
### What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false) <br> Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false). Filter records with pickup time in year 2019.
- 43244693