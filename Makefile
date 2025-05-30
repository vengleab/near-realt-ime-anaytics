start:
	docker compose up -d #--build

remove-minio-data:
	rm -rf ./minio/data

compose-down:
	docker compose down -v

down: compose-down remove-minio-data

minio-ui:
	open http://localhost:9001

pg:
	pgcli -h localhost -p 5432 -U postgres -d postgres

pg-src:
	curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '@./connectors/pg-src-connector.json'

s3-sink:
	curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '@./connectors/s3-sink.json'

re-sink:
	docker compose stop connect && \
	docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group connect-s3-sink \
  --topic debezium.commerce.products \
  --reset-offsets --to-earliest --execute && \
	docker compose start connect

connectors: pg-src s3-sink