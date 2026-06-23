BIN_DIR=bin
FETCHER_PKG = ./fetcher/main.go
ANALYZER_PKG = ./analyzer/main.go
EXECUTOR_PKG = ./executor/main.go
GEN_PROTO_DIR = /usr/include

.PHONY: \
all build format ci-fix up down db-clean dck-clean gen-proto test tools

all: build

build:
	@echo "=== Сборка ==="
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

up: down
	make format
	@echo "=== Запуск контейнеров ==="
	docker compose up

reup: down gen-proto format
	@echo "=== Перезапуск контейнеров ==="
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

tools:
	@echo "=== Установка зависимостей и утилит ==="
	go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh | sh -s -- -b $$(go env GOPATH)/bin v1.61.0
	@echo "Проверка protoc (если ошибка, установите по инструкции https://grpc.io/docs/protoc-installation/):"
	@which protoc && echo "protoc OK" || echo "protoc NOT FOUND"
