package main

import (
	"context"
	"log"
	"os"

	"github.com/fanoxiz/crypto-monitor/executor/adapters/db"
	"github.com/fanoxiz/crypto-monitor/executor/adapters/receiver"
	"github.com/fanoxiz/crypto-monitor/executor/config"
	"github.com/fanoxiz/crypto-monitor/executor/core"
)

func main() {
	log.SetPrefix("service=executor ")

	cfg, err := config.Load("executor/config.yaml")
	if err != nil {
		log.Fatalf("level=ERROR component=main event=config_load_failed err=\"%v\"", err)
	}

	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		dbUrl = "postgres://crypto_user:secret_password@localhost:5433/crypto_db"
	}

	repo, err := db.NewPostgresRepo(context.Background(), dbUrl, cfg.InitialBalance)
	if err != nil {
		log.Fatalf("level=ERROR component=main event=db_connect_failed err=\"%v\"", err)
	}

	executorService := core.NewExecutorService(repo, cfg.TradeSize, cfg.InitialBalance)
	grpcServer := receiver.NewGRPCReceiver(executorService)

	log.Printf("level=INFO component=main event=service_started port=%s", cfg.GRPCPort)
	if err := grpcServer.Start(cfg.GRPCPort); err != nil {
		log.Fatalf("level=ERROR component=main event=server_start_failed err=\"%v\"", err)
	}
}
