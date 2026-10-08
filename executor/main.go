package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fanoxiz/crypto-monitor/executor/adapters/db"
	"github.com/fanoxiz/crypto-monitor/executor/adapters/receiver"
	"github.com/fanoxiz/crypto-monitor/executor/config"
	"github.com/fanoxiz/crypto-monitor/executor/core"
)

func main() {
	log.SetPrefix("service=executor ")
	log.SetFlags(log.LstdFlags | log.LUTC)

	if err := run(); err != nil {
		log.Fatalf("level=ERROR component=main event=startup_failed err=%q", err)
	}
}

func run() error {
	cfg, err := config.Load("executor/config.yaml")
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	repo, err := db.NewPostgresRepo(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer repo.Close()

	service := core.NewExecutorService(repo)
	server := receiver.NewGRPCReceiver(service)

	return server.Start(ctx, cfg.GRPCPort)
}
