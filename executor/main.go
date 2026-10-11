package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/fanoxiz/crypto-monitor/executor/adapters/db"
	"github.com/fanoxiz/crypto-monitor/executor/adapters/receiver"
	"github.com/fanoxiz/crypto-monitor/executor/config"
	"github.com/fanoxiz/crypto-monitor/executor/core"
	"github.com/fanoxiz/crypto-monitor/internal/logging"
)

func main() {
	if err := logging.Configure("executor"); err != nil {
		slog.Error("logging_configuration_failed", "component", "main", "error", err)
		os.Exit(1)
	}

	if err := run(); err != nil {
		slog.Error("startup_failed", "component", "main", "error", err)
		os.Exit(1)
	}

	slog.Info("service_stopped", "component", "main")
}

func run() error {
	cfg, err := config.Load("executor/config.yaml")
	if err != nil {
		return err
	}

	slog.Info("configuration_loaded", "component", "main", "grpc_port", cfg.GRPCPort)

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
