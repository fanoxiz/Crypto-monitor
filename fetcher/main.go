package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fanoxiz/crypto-monitor/fetcher/adapters/api"
	"github.com/fanoxiz/crypto-monitor/fetcher/adapters/cache"
	"github.com/fanoxiz/crypto-monitor/fetcher/config"
	"github.com/fanoxiz/crypto-monitor/fetcher/core"
	"github.com/redis/go-redis/v9"
)

func main() {
	log.SetPrefix("service=fetcher ")
	log.SetFlags(log.LstdFlags | log.LUTC)

	if err := run(); err != nil {
		log.Fatalf("level=ERROR component=main event=startup_failed err=%q", err)
	}
}

func run() error {
	cfg, err := config.Load("fetcher/config.yaml")
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
