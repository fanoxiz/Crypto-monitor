package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/cache"
	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/sender"
	"github.com/fanoxiz/crypto-monitor/analyzer/config"
	"github.com/fanoxiz/crypto-monitor/analyzer/core"
	"github.com/fanoxiz/crypto-monitor/internal/logging"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := logging.Configure("analyzer"); err != nil {
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
	cfg, err := config.Load("analyzer/config.yaml")
	if err != nil {
		return err
	}

	slog.Info("configuration_loaded", "component", "main", "coins", cfg.TrackedCoins, "exchanges", cfg.Exchanges, "max_quote_age", cfg.MaxQuoteAge, "fees", cfg.Fees)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		return err
	}

	slog.Info("redis_connected", "component", "main")

	conn, err := grpc.NewClient(cfg.ExecutorGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	slog.Info("executor_client_created", "component", "main")

	store := cache.NewRedisQuoteStore(client, cfg.TrackedCoins, cfg.Exchanges)
	dealSender := sender.NewGRPCSender(conn)
	service := core.NewAnalyzerService(cfg.Fees, store, dealSender, cfg.MaxQuoteAge)

	service.Run(ctx)

	return nil
}
