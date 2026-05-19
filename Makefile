.PHONY: db-up db-down db-logs psql migrate docker-up docker-down docker-reset docker-logs build run-gateway run-ingester tidy test vet quality health smoke

SHELL := /bin/bash
PGDATABASE ?= gordios_collection_test
PG_URL ?= postgres://gordios:gordios@localhost:16432/$(PGDATABASE)?sslmode=disable
INGESTER_DB_URL ?= postgres://gordios_ingester:gordios_ingester@localhost:16432/$(PGDATABASE)?sslmode=disable
RAW_GATEWAY_DB_URL ?= postgres://gordios_raw_gateway:gordios_raw_gateway@localhost:16432/$(PGDATABASE)?sslmode=disable
GATEWAY_ADDR ?= :18080
GATEWAY_URL ?= http://localhost:18080
DB_CONTAINER ?= gordios-collection-test-db

db-up:
	docker compose up -d db

db-down:
	docker compose down

db-logs:
	docker compose logs -f db

psql:
	docker exec -it $(DB_CONTAINER) psql -U gordios -d $(PGDATABASE)

migrate:
	@for f in migrations/*.sql; do \
		echo "--> $$f"; \
		docker exec -i $(DB_CONTAINER) psql -U gordios -d $(PGDATABASE) -v ON_ERROR_STOP=1 -f - < "$$f" || exit 1; \
	done

docker-up:
	docker compose up -d --build gateway ingester

docker-down:
	docker compose down

docker-reset:
	docker compose down -v --remove-orphans

docker-logs:
	docker compose logs -f gateway ingester

tidy:
	go mod tidy

test:
	go test ./...

vet:
	go vet ./...

quality: test vet

build:
	mkdir -p bin
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/ingester ./cmd/ingester
	go build -o bin/importer ./cmd/importer

run-gateway:
	DATABASE_URL="$(RAW_GATEWAY_DB_URL)" GATEWAY_ADDR="$(GATEWAY_ADDR)" go run ./cmd/gateway

run-ingester:
	DATABASE_URL="$(INGESTER_DB_URL)" go run ./cmd/ingester

health:
	@curl -sf $(GATEWAY_URL)/healthz && echo

smoke:
	@for i in $$(seq 1 60); do \
		if curl -sf $(GATEWAY_URL)/readyz >/dev/null && curl -sf $(GATEWAY_URL)/healthz >/dev/null; then break; fi; \
		if [ "$$i" = "60" ]; then echo "gateway did not become healthy"; exit 1; fi; \
		sleep 2; \
	done
	@curl -sf $(GATEWAY_URL)/api/sources/status >/dev/null
	@curl -sf '$(GATEWAY_URL)/api/latest?source=emsc_seismic&max_age_min=1440&limit=1' >/dev/null
	@echo "collection smoke OK"
