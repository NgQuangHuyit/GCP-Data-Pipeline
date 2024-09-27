up:
	docker compose -f docker-compose-airflow.yml up

down:
	docker compose -f docker-compose-airflow.yml  down -v

init-dirs:
	mkdir -p ./dags ./logs ./plugins
	mkdir -p ./data/raw ./data/transformed
	mkdir ./credentials

setup:
	chmod +x enviroment_setup.sh
	./enviroment_setup.sh

