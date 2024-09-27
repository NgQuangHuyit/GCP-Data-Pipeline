from google.cloud import bigquery
import logging

from timeit import default_timer as timer
from numpy.f2py.auxfuncs import throw_error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


TABLE_SCHEMA = [
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("volume", "INTEGER"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
    ]


def bigquery_init(dataset_id: str, table_id: str, credential_path: str, **kwargs):
    client = bigquery.Client.from_service_account_json(credential_path)
    project_id = client.project
    full_dataset_id = f"{project_id}.{dataset_id}"
    full_table_id = f"{full_dataset_id}.{table_id}"
    dataset = bigquery.Dataset(full_dataset_id)

    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset {dataset.dataset_id} created at {dataset.created}")
    except Exception as e:
        throw_error(f"Failed to create dataset {full_dataset_id} due to exception: {e}")

    table = bigquery.Table(full_table_id, schema=TABLE_SCHEMA)

    try:
        table_instance =client.create_table(table, exists_ok=True)
        logger.info(f"Table {table_instance.full_table_id} created at {table_instance.created}")
        kwargs["ti"].xcom_push(key="full_table_id", value=table_instance.full_table_id)
    except Exception as e:
        throw_error(f"Failed to create table {full_table_id} due to exception: {e}")


def insert_gcs_blobs_to_bigquery(bucket_name: str, credential_path: str, **kwargs):
    blobs = kwargs["ti"].xcom_pull(task_ids="upload_to_gcs")
    table_id = kwargs["ti"].xcom_pull(task_ids="bigquery_init", key="full_table_id").replace(":", ".")

    logger.info(f"Inserting {blobs} to table {table_id}...")
    gcs_uris = [f"gs://{bucket_name}/{blob}" for blob in blobs]

    client = bigquery.Client.from_service_account_json(credential_path)

    config = bigquery.LoadJobConfig(
        schema=TABLE_SCHEMA,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV
    )

    try:
        timer
        use_timer = True
    except NameError:
        use_timer = False

    job = client.load_table_from_uri(gcs_uris, table_id, job_config=config)

    if use_timer:
        start = timer()

    job.result()

    if use_timer:
        end = timer()
        result_time = " in {0:.4f}s".format(end - start)
    else:
        result_time = ""

    logger.info(f"{job.output_rows} rows were loaded{result_time} to table{table_id}.")


if __name__ == "__main__":
    bigquery_init(dataset_id="nexar", table_id="stocktrades", credential_path="../credentials/key3.json")