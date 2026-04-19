package main

import (
	"context"
	"log"
	"os"

	"github.com/fanoxiz/crypto-monitor/executor/adapters/db"
	"github.com/fanoxiz/crypto-monitor/executor/adapters/receiver"
	"github.com/fanoxiz/crypto-monitor/executor/core"
)

const (
	tradeSize      = 1000.0
	initialBalance = 10000.0
)

func main() {

	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		dbUrl = "postgres://crypto_user:secret_password@localhost:5433/crypto_db"
	}

	repo, err := db.NewPostgresRepo(context.Background(), dbUrl, initialBalance)
	if err != nil {
		log.Fatalf("Ошибка подключения к БД: %v", err)
	}

	executorService := core.NewExecutorService(repo, tradeSize, initialBalance)
	httpServer := receiver.NewHTTPReceiver(executorService)

	if err := httpServer.Start("8082"); err != nil {
		log.Fatalf("Ошибка сервера: %v", err)
	}
}
