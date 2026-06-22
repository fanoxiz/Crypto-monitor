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
	tradeSize      = 10000.0
	initialBalance = 10000.0
)

func main() {
	log.SetPrefix("service=executor ")

	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		dbUrl = "postgres://crypto_user:secret_password@localhost:5433/crypto_db"
	}

	repo, err := db.NewPostgresRepo(context.Background(), dbUrl, initialBalance)
	if err != nil {
		log.Fatalf("level=ERROR component=main event=db_connect_failed err=\"%v\"", err)
	}

	executorService := core.NewExecutorService(repo, tradeSize, initialBalance)
	grpcServer := receiver.NewGRPCReceiver(executorService)

	log.Printf("level=INFO component=main event=service_started port=8082")
	if err := grpcServer.Start("8082"); err != nil {
		log.Fatalf("level=ERROR component=main event=server_start_failed err=\"%v\"", err)
	}
}
