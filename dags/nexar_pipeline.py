from datetime import datetime, timedelta
import os
import pytz
import shutil

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from scripts.bigquery import bigquery_init, insert_gcs_blobs_to_bigquery
from scripts.upload_to_gcs import concurrent_upload_blobs

vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')

#Big Query
DATASET_ID = os.getenv("BQ_DATASET_ID") if os.getenv("BQ_DATASET_ID") else "nexar"
TABLE_ID = os.getenv("BQ_TABLE_ID") if os.getenv("BQ_TABLE_ID") else "stocktrades"

#GOOGLE CLOUD STORAGE
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")


#GOOGLE DRIVE SOURCE
GG_DRIVE_FOLDER_ID = "1__k9gDSxeGauJhD6iBAoxRmhoO-p6WIG"

JSON_CREDENTIAL_PATH = os.getenv("JSON_CREDENTIAL_PATH") if os.getenv("JSON_CREDENTIAL_PATH") else "/opt/airflow/dags/credentials/my-key.json"
LOCAL_RAW_DATA_DIR = os.getenv("LOCAL_RAW_DATA_DIR") if os.getenv("LOCAL_RAW_DATA_DIR") else "/opt/airflow/dags/data/raw"
LOCAL_TRANSFORMED_DATA_DIR = os.getenv("LOCAL_TRANSFORMED_DATA_DIR") if os.getenv("LOCAL_TRANSFORMED_DATA_DIR") else "/opt/airflow/dags/data/transformed"


def clear_local_file(paths: list[str]):
    for path in paths:
        if os.path.exists(path):
            for item in os.listdir(path):
                item_path = os.path.join(path, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
            print(f"Đã xóa toàn bộ nội dung bên trong {path}")
        else:
            print(f"Thư mục {path} không tồn tại.")

default_agrs = {
    "owner": "nqh",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="nexar-pipeline",
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 9, 21, tzinfo=vietnam_tz),
    end_date=datetime(2024, 9, 25, tzinfo=vietnam_tz),
    catchup=True,
    default_args=default_agrs
) as dag:

    download_ggdrive_file = BashOperator(
        task_id='download_ggdrive_file',
        bash_command="""
        python {{ params.SCRIPTS }} \
        --execution-date {{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%d-%m-%Y') }} \
        --credential-file {{ params.CREDENTIAL_FILE }} \
        --gg-drive-folder-id {{ params.GG_DRIVE_FOLDER_ID }} \
        --target-dir {{ params.TARGET_DIR }}
    """,
    params={'GG_DRIVE_FOLDER_ID': GG_DRIVE_FOLDER_ID,
            'SCRIPTS': os.path.join(os.path.dirname(__file__), 'scripts', 'download_ggdriver_file.py'),
            'CREDENTIAL_FILE': JSON_CREDENTIAL_PATH,
            'TARGET_DIR': LOCAL_RAW_DATA_DIR}
    )

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command="""
            python {{ params.SCRIPTS }} \
            --input-path {{ params.RAW_PATH }}/{{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%d-%m-%Y') }} \
            --output-path {{ params.TRANSFORMED_PATH }}/{{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%d-%m-%Y') }}
        """,
        params={'SCRIPTS': os.path.join(os.path.dirname(__file__), 'scripts', 'transform_data.py'),
                'RAW_PATH': LOCAL_RAW_DATA_DIR,
                'TRANSFORMED_PATH': LOCAL_TRANSFORMED_DATA_DIR}
    )

    upload_to_gcs = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=concurrent_upload_blobs,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "json_credential_path": JSON_CREDENTIAL_PATH,
            "source_directory": os.path.join(LOCAL_TRANSFORMED_DATA_DIR, "{{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%d-%m-%Y') }}"),
            "blob_prefix": "{{ macros.datetime.strptime(ds, '%Y-%m-%d').strftime('%d-%m-%Y') }}/",
            "workers": 8
        },
        show_return_value_in_logs=True
    )

    bigquery_init_task = PythonOperator(
        task_id='bigquery_init',
        python_callable=bigquery_init,
        op_kwargs={
            "dataset_id": DATASET_ID,
            "table_id": TABLE_ID,
            "credential_path": JSON_CREDENTIAL_PATH
        }
    )

    insert_to_bigquery = PythonOperator(
        task_id='insert_to_bigquery',
        python_callable=insert_gcs_blobs_to_bigquery,
        op_kwargs={
            "bucket_name": BUCKET_NAME,
            "credential_path": JSON_CREDENTIAL_PATH
        }
    )

    clear_local_files = PythonOperator(
        task_id='clear_local_files',
        python_callable=clear_local_file,
        op_kwargs={
            "paths": [LOCAL_RAW_DATA_DIR, LOCAL_TRANSFORMED_DATA_DIR]
        }
    )

    download_ggdrive_file >> transform_data >> upload_to_gcs >> insert_to_bigquery >> clear_local_files
    bigquery_init_task >> insert_to_bigquery >> clear_local_files
