import os
from datetime import datetime
from io import BytesIO

import pandas as pd
from dotenv import load_dotenv
from minio import Minio

from src.config.logging_conf import logger

load_dotenv()

ENDPOINT_MINIO = os.getenv("ENDPOINT_MINIO")
ACCESS_KEY_MINIO = os.getenv("ACCESS_KEY_MINIO")
SECRET_KEY_MINIO = os.getenv("SECRET_KEY_MINIO")


def handle_etl_error(data: pd.DataFrame, bucket_name: str, table_name: str, process: str):
    """
    This function is used to handle error or invalid data by uploading the DataFrame to a MinIO bucket.
    """

    logger.error(f"Error etl process: {process}, table_name: {table_name}")
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Initialize MinIO client
    client = Minio(ENDPOINT_MINIO,
                   access_key=ACCESS_KEY_MINIO,
                   secret_key=SECRET_KEY_MINIO,
                   secure=False)

    # Make a bucket if it doesn't exist
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Convert DataFrame to CSV and then to bytes
    csv_bytes = data.to_csv().encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    # Upload the CSV file to the bucket
    res = client.put_object(
        bucket_name=bucket_name,
        object_name=f"{process}_{table_name}_{current_date}.csv",  # name the fail source name and current etl date
        data=csv_buffer,
        length=len(csv_bytes),
        content_type='application/csv'
    )

    logger.info(
        f"Error data uploaded to MinIO bucket, bucket_name: {bucket_name}, object_name: {res.object_name}, tag: {res.etag}")

    objects = client.list_objects(bucket_name)
    sorted_objects = sorted(objects, key=lambda obj: obj.last_modified, reverse=True)
    last_5_files = sorted_objects[:5]

    logger.info(f"Last 5 files on bucket: {bucket_name}")
    for obj in last_5_files:
        logger.info(f"File: {obj.object_name}, Last Modified: {obj.last_modified}")