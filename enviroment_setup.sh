echo "" > .env
echo "AIRFLOW_UID=$(id -u)" > .env
echo "AIRFLOW_GID=0" >> .env

echo "Set airflow www admit user"
read -p "Enter the user name: " username
echo "_AIRFLOW_WWW_USER_USERNAME=$username" >> .env

echo "Set airflow www admit password"
read -p "Enter the password: " password
echo  "_AIRFLOW_WWW_USER_PASSWORD=$password" >> .env

echo "Set GCS bucket name"
read -p "Enter the bucket name: " bucket_name
echo "GCS_BUCKET_NAME=$bucket_name" >> .env

read -p "Enter the bigquery dataset id: " dataset_id
echo "BQ_DATASET_ID=$dataset_id" >> .env

read -p "Enter the bigquery table id: " table_id
echo "BQ_TABLE_ID=$table_id" >> .env



