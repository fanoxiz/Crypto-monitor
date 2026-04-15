package main

import (
	"log"
	"net/http"
	"time"

	"github.com/fanoxiz/crypto-monitor/fetcher/adapters/api"
	"github.com/fanoxiz/crypto-monitor/fetcher/adapters/sender"
	"github.com/fanoxiz/crypto-monitor/fetcher/core"
)

func main() {
	reqFrequency := 2 * time.Second
	trackedCoins := []string{"BTC", "ETH"}
	urlToSend := "http://localhost:8081/prices"

	openedClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 2 * time.Second,
	}

	exchanges := []core.ExchangeAdapter{
		api.NewBinanceAdapter(openedClient),
		api.NewBybitAdapter(openedClient),
		api.NewCoinbaseAdapter(openedClient),
		// adapters.NewKrakenAdapter(openedClient), doesn't work in Russia
		api.NewOKXAdapter(openedClient),
	}

	senderService := sender.NewSenderService(openedClient, urlToSend)
	fetcherService := core.NewFetcherService(exchanges, senderService)

	log.Println("Fetcher service is running...")
	fetcherService.Start(trackedCoins, reqFrequency)
}
