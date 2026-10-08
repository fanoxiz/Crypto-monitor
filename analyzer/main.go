package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/cache"
	"github.com/fanoxiz/crypto-monitor/analyzer/config"
	"github.com/fanoxiz/crypto-monitor/analyzer/core"
	"github.com/redis/go-redis/v9"
)

func main() {
	log.SetPrefix("service=analyzer ")
	log.SetFlags(log.LstdFlags | log.LUTC)

	if err := run(); err != nil {
		log.Fatalf("level=ERROR component=main event=startup_failed err=%q", err)
	}
}

func run() error {
	cfg, err := config.Load("analyzer/config.yaml")
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		return err
	}

	store := cache.NewRedisQuoteStore(client, cfg.TrackedCoins, cfg.Exchanges)
	service := core.NewAnalyzerService(cfg.Fees, store, cfg.MaxQuoteAge)

	service.Run(ctx)

	return nil
}
