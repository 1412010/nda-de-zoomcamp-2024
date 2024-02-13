# Solution for homework of Week-03

<b>SETUP:</b></br>
Create an external table using the Green Taxi Trip Records Data for 2022. </br>
Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table). </br>
</p>

### Solution:

> Refer to this script for automatically download and upload data to GCS bucket: [week-03\green_trip_data_to_gcs.py](week-03\green_trip_data_to_gcs.py)

+ SQL for creating external table ```external_green_tripdata_2022```:

  ```sql
  CREATE OR REPLACE EXTERNAL TABLE `nda-de-zoomcamp.ny_taxi_trips.external_green_tripdata_2022`
  OPTIONS (
    format = 'PARQUET',
    uris = ['gs://nda-de-zoomcamp-bucket/data/green/2020/green_tripdata_2022-*.parquet']
  );
  ```

+ SQL for creating table ```green_tripdata_2022_non_partitoned```:

  ```sql
  CREATE OR REPLACE TABLE
    `nda-de-zoomcamp.ny_taxi_trips.green_tripdata_2022_non_partitoned` AS
  SELECT
    *
  FROM
    `nda-de-zoomcamp.ny_taxi_trips.external_green_tripdata_2022`
  ```

## Question 1:

Question 1: What is count of records for the 2022 Green Taxi Data??

+ 65,623,481
+ 840,402
+ 1,936,423
+ 253,647

&rarr; Solution:

  ```sql
  SELECT COUNT(*) 
  FROM `nda-de-zoomcamp.ny_taxi_trips.green_tripdata_2022_non_partitoned`
  ```

+ Results: ```840,402```

## Question 2:

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.</br>
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

+ 0 MB for the External Table and 6.41MB for the Materialized Table
+ 18.82 MB for the External Table and 47.60 MB for the Materialized Table
+ 0 MB for the External Table and 0MB for the Materialized Table
+ 2.14 MB for the External Table and 0MB for the Materialized Table

&rarr; Solution:

+ Query External table:

  ```sql
  SELECT COUNT(DISTINCT PULocationID)
  FROM
  `nda-de-zoomcamp.ny_taxi_trips.external_green_tripdata_2022`
  ```

  + Check the query measurement on the top right corner.
  + Result: ```0 MB```

+ Query Materialized table:

  ```sql
  SELECT COUNT(DISTINCT PULocationID)
  FROM
  `nda-de-zoomcamp.ny_taxi_trips.green_tripdata_2022_non_partitoned`
  ```

  + Check the query measurement on the top right corner.
  + Result: ```6.41 MB```

+ Answer:

## Question 3:

How many records have a fare_amount of 0?

+ 12,488
+ 128,219
+ 112
+ 1,622

&rarr; Solution:

+ SQL query:

  ```sql
  SELECT COUNT(*)
  FROM
    `nda-de-zoomcamp.ny_taxi_trips.green_tripdata_2022_non_partitoned`
  WHERE fare_amount = 0
  ```

+ Results: ```1,622```

## Question 4:

What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

+ Cluster on lpep_pickup_datetime Partition by PUlocationID
+ Partition by lpep_pickup_datetime  Cluster on PUlocationID
+ Partition by lpep_pickup_datetime and Partition by PUlocationID
+ Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

&rarr; Solution:

+ SQL query for creating clustered and partitioned table:

  ```sql
  CREATE OR REPLACE TABLE `nda-de-zoomcamp.ny_taxi_trips.green_tripdata_2022_clustered_partitioned`
  PARTITION BY DATE(lpep_pickup_datetime)
  CLUSTER BY PUlocationID 
  AS
  SELECT * 
  FROM `nda-de-zoomcamp.ny_taxi_trips.external_green_tripdata_2022`
  ```

## Question 5:

Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime
06/01/2022 and 06/30/2022 (inclusive)</br>

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? </br>

Choose the answer which most closely matches.</br>

+ 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
+ 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
+ 5.63 MB for non-partitioned table and 0 MB for the partitioned table
+ 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

&rarr; Solution:

+ Query non-partitioned table:

  ```sql
  SELECT DISTINCT PULocationID 
  FROM `nda-de-zoomcamp.ny_taxi_trips.green_tripdata_2022_non_partitoned`
  WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
  ```

  + Results: ```12.82 MB```

+ Query partitioned table:

  ```sql
  SELECT DISTINCT PULocationID 
  FROM `nda-de-zoomcamp.ny_taxi_trips.green_tripdata_2022_clustered_partitioned`
  WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
  ```

  + Results: ```1.12 MB```

## Question 6:

Where is the data stored in the External Table you created?

+ Big Query
+ GCP Bucket
+ Big Table
+ Container Registry

&rarr; Solution: GCP Bucket. Bigquery does not store data of external table, only the schema and metadata.

## Question 7:

It is best practice in Big Query to always cluster your data:

+ True
+ False

&rarr; Solution: False. Depends on the circumstances.

## (Bonus) Question 8:

No Points: Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

&rarr; Solution: 

+ Query statement: 

  ```sql
  SELECT COUNT(*)
  FROM `nda-de-zoomcamp.ny_taxi_trips.green_tripdata_2022_non_partitoned`
  ```

+ Check the query estimamte, it is saying *"This query will processed 0B when run"*
+ Because the number of rows is already saved in the metadata of the table when created, hence this query does not scan the data, instead it returns the metadata.
