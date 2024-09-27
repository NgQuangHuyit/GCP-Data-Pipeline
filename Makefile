up:
	docker-compose up -d

down:
	docker-compose down -v

init-dirs:
	mkdir -p ./dags ./logs ./plugins
	mkdir -p ./data/raw ./data/transformed
	mkdir ./credentials

setup:
	chmod +x enviroment_setup.sh
	./enviroment_setup.sh

