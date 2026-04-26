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
	cfg, err := config.Load("analyzer/config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
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
		log.Fatalf("failed to connect redis: %v", err)
	}

	priceStore := cache.NewRedisPriceStore(redisClient, cfg.RedisKeyPrefix)
	analyzerService := core.NewAnalyzerService(cfg.Fees, senderService, priceStore)
	receiver := receiver.NewHTTPReceiver(analyzerService)

	log.Println("Analyzer service is running...")
	if err := receiver.Start(cfg.Port); err != nil {
		log.Fatalf("Ошибка запуска сервера: %v", err)
	}
}
