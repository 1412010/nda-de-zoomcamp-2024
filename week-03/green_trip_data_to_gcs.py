from google.cloud import storage
import os
import wget

def upload_blob(bucket_name, source_file_name, destination_blob_name, credentials_file):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client.from_service_account_json(credentials_file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    generation_match_precondition = 0

    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

CREDENTIAL_PATH = "week-03/nda-de-zoomcamp-060ebbb8d83e.json"
url_format = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{month:02d}.parquet"
filename_format = "green_tripdata_2022-{month:02d}.parquet"

for month in range(1, 13):
    url = url_format.format(month=month)
    filename = filename_format.format(month=month)
    filepath = os.path.join(os.curdir, "week-03\data", filename)
    
    # Dowload the parquet file
    response = wget.download(url, filepath)
    
    upload_blob("nda-de-zoomcamp-bucket", filepath, f"data/green/2022/{filename}", CREDENTIAL_PATH)
    