package main

import (
	"log"
	"net/http"
	"time"

	"github.com/fanoxiz/crypto-monitor/fetcher/adapters/api"
	"github.com/fanoxiz/crypto-monitor/fetcher/adapters/sender"
	"github.com/fanoxiz/crypto-monitor/fetcher/config"
	"github.com/fanoxiz/crypto-monitor/fetcher/core"
)

func main() {
	cfg, err := config.Load("fetcher/config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	exchangeClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: cfg.HTTPClientTimeout,
	}

	analyzerClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        20,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: cfg.HTTPClientTimeout,
	}

	exchanges := []core.ExchangeAdapter{
		api.NewBinanceAdapter(exchangeClient),
		api.NewBybitAdapter(exchangeClient),
		api.NewBitgetAdapter(exchangeClient),
		api.NewCoinbaseAdapter(exchangeClient),
		api.NewOKXAdapter(exchangeClient),
	}

	poolCfg := (core.WorkerPoolConfig{}).Precalculate(
		len(cfg.TrackedCoins),
		len(exchanges),
		cfg.RequestFrequency,
		cfg.HTTPClientTimeout,
	)

	senderService := sender.NewSenderService(analyzerClient, cfg.AnalyzerEndpoint)
	fetcherService := core.NewFetcherService(exchanges, senderService, poolCfg)

	log.Println("Fetcher service is running...")
	fetcherService.Start(cfg.TrackedCoins, cfg.RequestFrequency)
}
