# Solution for homework of Week-02

### Assignment

The goal will be to construct an ETL pipeline that loads the data, performs some transformations, and writes the data to a database (and Google Cloud!).

- Create a new pipeline, call it `green_taxi_etl`
- Add a data loader block and use Pandas to read data for the final quarter of 2020 (months `10`, `11`, `12`).
  - You can use the same datatypes and date parsing methods shown in the course.
  - `BONUS`: load the final three months using a for loop and `pd.concat`
- Add a transformer block and perform the following:
  - Remove rows where the passenger count is equal to 0 _and_ the trip distance is equal to zero.
  - Create a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date.
  - Rename columns in Camel Case to Snake Case, e.g. `VendorID` to `vendor_id`.
  - Add three assertions:
    - `vendor_id` is one of the existing values in the column (currently)
    - `passenger_count` is greater than 0
    - `trip_distance` is greater than 0
- Using a Postgres data exporter (SQL or Python), write the dataset to a table called `green_taxi` in a schema `mage`. Replace the table if it already exists.
- Write your data as Parquet files to a bucket in GCP, partioned by `lpep_pickup_date`. Use the `pyarrow` library!
- Schedule your pipeline to run daily at 5AM UTC.

### Questions

## Question 1. Data Loading

Once the dataset is loaded, what's the shape of the data?

* 266,855 rows x 20 columns
* 544,898 rows x 18 columns
* 544,898 rows x 20 columns
* 133,744 rows x 20 columns

### Solution:

Code for loading data:

```python
@data_loader
def load_data_from_api(*args, **kwargs):
  """
  Template for loading data from API
  """
  color = 'green'
  year = 2020
  months = [10, 11, 12]

  list_df = []
  for month in months:
      url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02d}.csv.gz'
      print(url)
      taxi_dtypes = {
          'VendorID': pd.Int64Dtype(),
          'passenger_count': pd.Int64Dtype(),
          'trip_distance': float,
          'RatecodeID': pd.Int64Dtype(),
          'store_and_fwd_flag': str,
          'PULocationID': pd.Int64Dtype(),
          'DOLocationID': pd.Int64Dtype(),
          'payment_type': pd.Int64Dtype(),
          'fare_amount': float,
          'extra': float,
          'mta_tax': float,
          'tip_amount': float,
          'tolls_amount': float,
          'improvement_surcharge': float,
          'total_amount': float,
          'congestion_surcharge': float 
      }

      if color == 'yellow':
          parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
      else:  # green
          parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

      df = pd.read_csv(url, sep=",", compression="gzip", 
                      dtype=taxi_dtypes, parse_dates=parse_dates)
      list_df.append(df.copy())
  
  df = pd.concat(list_df)
  print(f'Data shape: {len(df)} rows x {len(df.columns)} columns')
  return df
  ```

Result: 266855 rows x 20 columns

## Question 2. Data Transformation

Upon filtering the dataset where the passenger count is greater than 0 _and_ the trip distance is greater than zero, how many rows are left?

* 544,897 rows
* 266,855 rows
* 139,370 rows
* 266,856 rows

### Solution:

Code for transforming data: 

  ```python
  @transformer
  def transform(data, *args, **kwargs):
      # count = data[data['passenger_count'] == 0 or data['trip_distance'] == 0]
      print('Processing: Remove trips with passenger_count = 0 and trip_distance = 0 ')
      data = data[(data['passenger_count'] > 0)]
      data = data[(data['trip_distance'] > 0)]

      data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
      data['lpep_dropoff_date'] = data['lpep_dropoff_datetime'].dt.date
      
      cols_before_rename = data.columns

      data.columns = (data.columns
                          .str.replace(' ', '_')
                          .str.lower()
      )
      data = data.rename(columns={'vendorid': 'vendor_id'})
      cols_after_rename = data.columns

      print(f'Data after transformation: {len(data)} rows x {len(data.columns)} columns')
      print("vendor_id unique values: ", list(data['vendor_id'].unique()))
      print('number of columns to rename to snake case: ', len(set(cols_before_rename) - set(cols_after_rename)))
      return data
  ```

Result: 139,370 rows

## Question 3. Data Transformation

Which of the following creates a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date?

* `data = data['lpep_pickup_datetime'].date`
* `data('lpep_pickup_date') = data['lpep_pickup_datetime'].date`
* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date`
* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt().date()`

### Solution:
  ```python
  data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
  ```

## Question 4. Data Transformation

What are the existing values of `VendorID` in the dataset?

* 1, 2, or 3
* 1 or 2
* 1, 2, 3, 4
* 1

### Solution:

Code to get values:

  ```python
  print("vendor_id unique values: ", list(data['vendor_id'].unique()))
  ```

Result: ```[1, 2]``` -> 1 or 2

## Question 5. Data Transformation

How many columns need to be renamed to snake case?

* 3
* 6
* 2
* 4

### Solution:

From the output of the code in question 2: ```4```

## Question 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?

* 96
* 56
* 67
* 108

### Solution:

Code to export data to GCS: 

  ```python
  import os
  import pyarrow as pa
  import pyarrow.parquet as pq

  if 'data_exporter' not in globals():
      from mage_ai.data_preparation.decorators import data_exporter

  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/nda-de-zoomcamp-060ebbb8d83e.json'

  bucket_name = 'nda-de-zoomcamp-bucket'
  project_id = 'nda-de-zoomcamp'
  table_name = 'ny_green_taxi_data_partition'

  root_path = f"{bucket_name}/{table_name}"

  @data_exporter
  def export_data(data, *args, **kwargs):

      table = pa.Table.from_pandas(data)
      gcs = pa.fs.GcsFileSystem()

      pq.write_to_dataset(
          table,
          root_path=root_path,
          partition_cols=['lpep_pickup_date'],
          filesystem=gcs
      )
  ```

Go to GCS bucket, and you can see the number of folders: ```96```
