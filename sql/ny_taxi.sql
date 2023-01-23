truncate table yellow_tripdata_trip

drop table yellow_tripdata_trip

select * from yellow_tripdata_trip limit 100;

select distinct airport_fee
from yellow_tripdata_trip


--How many taxi trips were there on January 15

select count(*) 
from yellow_tripdata_trip 
where date(tpep_pickup_datetime) = '2021-01-15'




