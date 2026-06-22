BIN_DIR=bin
FETCHER_PKG = ./fetcher/main.go
ANALYZER_PKG = ./analyzer/main.go
EXECUTOR_PKG = ./executor/main.go

.PHONY: \
all build format ci-fix up down db-clean dck-clean gen-proto

GEN_PROTO_DIR = /tmp/protoc/include

all: build

build:
	@echo "=== Сборка ==="
	make dck-clean
	make format
	go build -o $(BIN_DIR)/fetcher $(FETCHER_PKG)
	go build -o $(BIN_DIR)/analyzer $(ANALYZER_PKG)
	go build -o $(BIN_DIR)/executor $(EXECUTOR_PKG)

format:
	go mod tidy
	go mod download
	gofmt -s -w .

ci-fix:
	golangci-lint run --fix

gen-proto:
	protoc \
		--proto_path=. \
		--proto_path=$(GEN_PROTO_DIR) \
		--go_out=. \
		--go_opt=module=github.com/fanoxiz/crypto-monitor \
		--go-grpc_out=. \
		--go-grpc_opt=module=github.com/fanoxiz/crypto-monitor \
		contracts/service.proto

up:
	make format
	@echo "=== Запуск контейнеров ==="
	docker compose up

reup:
	make format
	@echo "=== Перезапуск контейнеров ==="
	make down
	docker compose up --build

down:
	@echo "=== Остановка контейнеров ==="
	docker compose down

db-clean:
	@echo "=== Остановка контейнеров ==="
	docker compose down -v

dck-clean:
	@echo "=== Чистка docker ==="
	docker compose down -v --remove-orphans
	docker image prune -f
