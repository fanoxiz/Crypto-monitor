BIN_DIR ?= bin
GEN_PROTO_DIR ?= /usr/include
TEST_TIMEOUT ?= 120s
GO_DIRS := analyzer contracts executor fetcher internal tests

.PHONY: build clean format check \
	test test-integration \
	gen-proto tools up reup down logs db-clean

build:
	mkdir -p "$(BIN_DIR)"
	go build -o "$(BIN_DIR)/fetcher" ./fetcher
	go build -o "$(BIN_DIR)/analyzer" ./analyzer
	go build -o "$(BIN_DIR)/executor" ./executor

clean:
	rm -f "$(BIN_DIR)/fetcher" "$(BIN_DIR)/analyzer" "$(BIN_DIR)/executor"

format:
	gofmt -w $(GO_DIRS)
	go mod tidy

check:
	go vet ./...

test:
	go test -count=1 -timeout="$(TEST_TIMEOUT)" ./...

test-integration:
	@TEST_TIMEOUT="$(TEST_TIMEOUT)" bash tests/run.sh

gen-proto:
	protoc \
		--proto_path=. \
		--proto_path="$(GEN_PROTO_DIR)" \
		--go_out=. \
		--go_opt=module=github.com/fanoxiz/crypto-monitor \
		--go-grpc_out=. \
		--go-grpc_opt=module=github.com/fanoxiz/crypto-monitor \
		contracts/service.proto

up:
	docker compose up --build

reup:
	$(MAKE) down
	$(MAKE) up

down:
	docker compose down

logs:
	docker compose logs -f fetcher analyzer executor

db-clean:
	docker compose down -v

tools:
	command -v protoc
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
