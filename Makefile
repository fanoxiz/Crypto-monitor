BIN_DIR ?= bin
GEN_PROTO_DIR ?= /usr/include
GO ?= go
PROTOC ?= protoc
COMPOSE ?= docker compose

.PHONY: all build format tidy check test gen-proto up reup down logs db-clean dck-clean tools

all: build

build:
	mkdir -p "$(BIN_DIR)"
	$(GO) build -o "$(BIN_DIR)/fetcher" ./fetcher
	$(GO) build -o "$(BIN_DIR)/analyzer" ./analyzer
	$(GO) build -o "$(BIN_DIR)/executor" ./executor

format:
	gofmt -w analyzer contracts executor fetcher

tidy:
	$(GO) mod tidy

check:
	$(GO) vet ./...

test:
	$(GO) test ./...

gen-proto:
	$(PROTOC) \
		--proto_path=. \
		--proto_path="$(GEN_PROTO_DIR)" \
		--go_out=. \
		--go_opt=module=github.com/fanoxiz/crypto-monitor \
		--go-grpc_out=. \
		--go-grpc_opt=module=github.com/fanoxiz/crypto-monitor \
		contracts/service.proto

up:
	$(COMPOSE) up --build

reup: up

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f fetcher analyzer executor

db-clean:
	$(COMPOSE) down -v

dck-clean:
	$(COMPOSE) down -v --remove-orphans --rmi local

tools:
	command -v $(PROTOC)
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
