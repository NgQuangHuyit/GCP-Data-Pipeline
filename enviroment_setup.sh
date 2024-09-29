echo "" > .env
echo "AIRFLOW_UID=$(id -u)" > .env
echo "AIRFLOW_GID=0" >> .env


read -p "Enter airflow www user name: " username
echo "_AIRFLOW_WWW_USER_USERNAME=$username" >> .env


read -p "Enter airflow www password: " password
echo  "_AIRFLOW_WWW_USER_PASSWORD=$password" >> .env

read -p "Enter the json credential path: " credentials_path
echo "JSON_CREDENTIAL_PATH=$credentials_path" >> .env

read -p "Enter the GCS bucket name: " bucket_name
echo "GCS_BUCKET_NAME=$bucket_name" >> .env

read -p "Enter the bigquery dataset id: " dataset_id
echo "BQ_DATASET_ID=$dataset_id" >> .env

read -p "Enter the bigquery table id: " table_id
echo "BQ_TABLE_ID=$table_id" >> .env



