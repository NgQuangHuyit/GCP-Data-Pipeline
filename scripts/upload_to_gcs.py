import os
import logging

import google.cloud.storage.transfer_manager
from google.cloud import storage
from google.cloud.storage import transfer_manager


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def concurrent_upload_blobs(
        bucket_name,
        json_credential_path,
        source_directory="",
        blob_prefix="",
        workers=8):

    storage_client = storage.Client.from_service_account_json(json_credential_path)
    bucket = storage_client.bucket(bucket_name)
    file_blob_pairs= []
    for filename in os.listdir(source_directory):
        blob = bucket.blob(blob_prefix + filename)
        absolute_filename = os.path.join(source_directory, filename)
        file_blob_pairs.append((absolute_filename, blob))

    results = transfer_manager.upload_many(file_blob_pairs,
                                           skip_if_exists=True,
                                           max_workers=workers,
                                           worker_type=google.cloud.storage.transfer_manager.THREAD)
    for (filename, blob), result in zip(file_blob_pairs, results):
        if isinstance(result, Exception):
            logger.error(f"Failed to upload {filename} due to exception: {result}")
        else:
            logger.info(f"Uploaded {filename} to {blob.name}.")

    blobs = storage_client.list_blobs(bucket_or_name=bucket_name, prefix=blob_prefix)
    results = [blob.name for blob in blobs]
    return results



if __name__ == "__main__":
    for filename in os.listdir('/opt/airflow/dags/data/transformed/21-09-2024'):
        print(filename)