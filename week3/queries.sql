-- #Week 3 homework dtc data engineering course 2023


-- Q1: COUNT records for 2019

-- SELECT COUNT(dispatching_base_num) FROM `dtc-de-zoomcamp-376519.fhv_rides.fhv2019_external` 
-- LIMIT 100;

-- SELECT COUNT(dispatching_base_num) FROM `dtc-de-zoomcamp-376519.fhv_rides.fhv2019_native`
-- LIMIT 100;


-- Q2: COUNT DISTINCT number of affiliated_base_number

-- SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dtc-de-zoomcamp-376519.fhv_rides.fhv2019_external`
-- LIMIT 10;

-- SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dtc-de-zoomcamp-376519.fhv_rides.fhv2019_native`
-- LIMIT 10;


-- Q3: How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

-- SELECT SUM(CASE WHEN PUlocationID IS NULL and DOlocationID IS NULL THEN 1 ELSE 0 END) FROM `dtc-de-zoomcamp-376519.fhv_rides.fhv2019_native`
-- LIMIT 10;


-- Q4: What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
-- Answer: partition by pickup_datetime (using day or month), cluster by affiliated_base_number. 
-- Partitioning in BQ is only possible on cols using time values, which is a give-away for this question


-- Q5: Write a query to retrieve the DISTINCT affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive) using table based on Q4

-- SELECT DISTINCT Affiliated_base_number
-- FROM `dtc-de-zoomcamp-376519.fhv_rides.fhv2019_native_part_clust` 
-- WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
-- LIMIT 1000;

-- Select DISTINCT Affiliated_base_number
-- FROM `dtc-de-zoomcamp-376519.fhv_rides.fhv2019_native` 
-- WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
-- LIMIT 10000;


-- Q6: Where is the data stored in the External Table you created?
-- Answer: in the gcp bucket as shown in the source section of the details tab of the table


-- Q7: Is it best practice in Big Query to always cluster your data?
-- Answer: No, clustering prevents BQ from correctly estimating query costs which can be a problem in certain use cases. Clustering also bloats a table (specifically its metadata) that is smaller than ~ 1GB, which prevents performance gain.