BIN_DIR=bin
FETCHER_PKG = ./fetcher/main.go
ANALYZER_PKG = ./analyzer/main.go
EXECUTOR_PKG = ./executor/main.go

.PHONY: \
all build format ci-fix run-fetcher run-analyzer run-executor \
up down dck-clean

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

run-fetcher:
	@echo "=== Запуск сервиса fetcher ==="
	go run $(FETCHER_PKG)

run-analyzer:
	@echo "=== Запуск сервиса analyzer ==="
	go run $(ANALYZER_PKG)

run-executor:
	@echo "=== Запуск сервиса executor ==="
	go run $(EXECUTOR_PKG)

up:
	make dck-clean
	make format
	@echo "=== Запуск контейнеров ==="
	docker compose up --build

down:
	@echo "=== Остановка контейнеров ==="
	docker compose down

dck-clean:
	@echo "=== Чистка docker ==="
	docker compose down -v --remove-orphans
	docker image prune -f
