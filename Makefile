.PHONY: build


presto:
	@./trino --debug --user=admin --password --truststore-path=./certs/truststore.jks --truststore-password=password --server https://localhost:8443

presto-k8s:
	@./trino --debug --user=admin --password --truststore-path=./certs/truststore.jks --truststore-password=password --server https://coordinator:32538

start: up
up:
	@docker-compose --env-file .env up -d

stop: down
down:
	@docker-compose down

ps: status
status:
	@docker-compose ps

hive-cli:
	@docker exec -it hive hive

psql:
	@docker exec -it postgres psql -U postgres
