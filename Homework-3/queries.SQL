CREATE OR REPLACE EXTERNAL TABLE `data-engineering-course-412921.green_taxi_nyc_2022.my_table`
  OPTIONS (
    format ="CSV",
    uris = ['gs://data-engineering-course-nyc-data/green-nyc-taxi-2022/data.csv']
    );

SELECT COUNT(DISTINCT(PULocationID)) AS unique FROM data-engineering-course-412921.green_taxi_nyc_2022.table_bq;


SELECT COUNT(DISTINCT(PULocationID)) AS unique FROM data-engineering-course-412921.green_taxi_nyc_2022.my_table;

SELECT COUNT(fare_amount)
FROM data-engineering-course-412921.green_taxi_nyc_2022.my_table
WHERE fare_amount=0; 

SELECT DISTINCT(PULocationID)
FROM data-engineering-course-412921.green_taxi_nyc_2022.table_bq
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP('2022-01-06') AND TIMESTAMP('2022-01-31');

CREATE OR REPLACE TABLE `data-engineering-course-412921.green_taxi_nyc_2022.partitionated` 
PARTITION BY DATE(lpep_pickup_datetime) AS SELECT * FROM data-engineering-course-412921.green_taxi_nyc_2022.table_bq;

CREATE OR REPLACE TABLE `data-engineering-course-412921.green_taxi_nyc_2022.partitionated_from_external_table` 
PARTITION BY DATE(lpep_pickup_datetime) AS SELECT * FROM data-engineering-course-412921.green_taxi_nyc_2022.my_table;

SELECT DISTINCT(PULocationID)
FROM data-engineering-course-412921.green_taxi_nyc_2022.partitionated
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP('2022-01-06') AND TIMESTAMP('2022-01-31');

SELECT DISTINCT(PULocationID)
FROM data-engineering-course-412921.green_taxi_nyc_2022.partitionated_from_external_table
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP('2022-01-06') AND TIMESTAMP('2022-01-31');

