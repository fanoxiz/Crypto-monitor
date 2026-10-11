package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/fanoxiz/crypto-monitor/fetcher/adapters/api"
	"github.com/fanoxiz/crypto-monitor/fetcher/adapters/cache"
	"github.com/fanoxiz/crypto-monitor/fetcher/config"
	"github.com/fanoxiz/crypto-monitor/fetcher/core"
	"github.com/fanoxiz/crypto-monitor/internal/logging"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := logging.Configure("fetcher"); err != nil {
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
	cfg, err := config.Load("fetcher/config.yaml")
	if err != nil {
		return err
	}

	slog.Info("configuration_loaded", "component", "main", "coins", cfg.TrackedCoins, "price_ttl", cfg.PriceTTL)

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

	exchanges := []core.Exchange{
		api.BinanceAdapter{
			URL: cfg.BinanceURL,
		},
		api.BybitAdapter{
			URL: cfg.BybitURL,
		},
		api.BitgetAdapter{
			URL: cfg.BitgetURL,
		},
		api.OKXAdapter{
			URL: cfg.OKXURL,
		},
	}

	store := cache.NewRedisQuoteStore(client, cfg.PriceTTL)
	service := core.NewFetcherService(exchanges, store)

	service.Run(ctx, cfg.TrackedCoins)

	return nil
}
