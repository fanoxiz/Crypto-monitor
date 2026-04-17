package main

import (
	"log"
	"net/http"
	"time"

	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/receiver"
	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/sender"
	"github.com/fanoxiz/crypto-monitor/analyzer/config"
	"github.com/fanoxiz/crypto-monitor/analyzer/core"
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
	analyzerService := core.NewAnalyzerService(cfg.Fees, senderService)
	receiver := receiver.NewHTTPReceiver(analyzerService)

	if err := receiver.Start(cfg.Port); err != nil {
		log.Fatalf("Ошибка запуска сервера: %v", err)
	}
}
