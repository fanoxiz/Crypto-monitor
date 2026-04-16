package main

import (
	"log"

	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/receiver"
	"github.com/fanoxiz/crypto-monitor/analyzer/config"
	"github.com/fanoxiz/crypto-monitor/analyzer/core"
)

func main() {
	cfg, err := config.Load("analyzer/config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	analyzerService := core.NewAnalyzerService(cfg.Fees)
	receiver := receiver.NewHTTPReceiver(analyzerService) // One day - NewgRPCReceiver

	if err := receiver.Start(cfg.Port); err != nil {
		log.Fatalf("Ошибка запуска сервера: %v", err)
	}
}
