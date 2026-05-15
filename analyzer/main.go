package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/cache"
	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/receiver"
	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/sender"
	"github.com/fanoxiz/crypto-monitor/analyzer/config"
	"github.com/fanoxiz/crypto-monitor/analyzer/core"
	"github.com/redis/go-redis/v9"
)

func main() {
	log.SetPrefix("service=analyzer ")

	cfg, err := config.Load("analyzer/config.yaml")
	if err != nil {
		log.Fatalf("level=ERROR component=main event=config_load_failed err=\"%v\"", err)
	}

	openedClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: cfg.HTTPClientTimeout,
	}

	senderService := sender.NewSenderService(openedClient, cfg.ExecutorEndpoint)

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	defer redisClient.Close()

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("level=ERROR component=main event=redis_connect_failed err=\"%v\"", err)
	}

	priceStore := cache.NewRedisPriceStore(redisClient, cfg.RedisKeyPrefix, cfg.PriceTTL)
	analyzerService := core.NewAnalyzerService(cfg.Fees, senderService, priceStore)
	receiver := receiver.NewHTTPReceiver(analyzerService)

	log.Printf("level=INFO component=main event=service_started port=%s", cfg.Port)
	if err := receiver.Start(cfg.Port); err != nil {
		log.Fatalf("level=ERROR component=main event=server_start_failed err=\"%v\"", err)
	}
}
