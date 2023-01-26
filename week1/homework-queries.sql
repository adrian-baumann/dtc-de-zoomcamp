-- Q3 
select COUNT(1) from green_taxi_data 
where lpep_pickup_datetime::date = '2019-01-15'::date and lpep_dropoff_datetime::date = '2019-01-15'::date;

-- Q4
select lpep_pickup_datetime::date, max(trip_distance) 
from green_taxi_data 
group by 1 order by 2 desc
limit 10;

-- Q5
select passenger_count, COUNT(1) 
from green_taxi_data 
where lpep_pickup_datetime::date = '2019-01-01'::date 
group by 1 order by 1
limit 10;

-- Q6
select tzd."Zone", max(gtd.tip_amount) as largest_tip
from green_taxi_data gtd 
inner join taxi_zone_data tzd on gtd."DOLocationID" = tzd."LocationID" 
where gtd."PULocationID" in (select "LocationID" from taxi_zone_data where "Zone" = 'Astoria')
group by 1 order by 2 desc;