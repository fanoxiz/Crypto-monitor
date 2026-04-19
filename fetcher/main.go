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

	openedClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: cfg.HTTPClientTimeout,
	}

	exchanges := []core.ExchangeAdapter{
		api.NewBinanceAdapter(openedClient),
		api.NewBybitAdapter(openedClient),
		api.NewBitgetAdapter(openedClient),
		api.NewCoinbaseAdapter(openedClient),
		api.NewOKXAdapter(openedClient),
	}

	senderService := sender.NewSenderService(openedClient, cfg.AnalyzerEndpoint)
	fetcherService := core.NewFetcherService(exchanges, senderService)

	log.Println("Fetcher service is running...")
	fetcherService.Start(cfg.TrackedCoins, cfg.RequestFrequency)
}
