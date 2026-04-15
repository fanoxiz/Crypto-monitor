package main

import (
	"log"
	"net/http"
	"time"

	"github.com/fanoxiz/crypto-monitor/services/fetcher/adapters/api"
	"github.com/fanoxiz/crypto-monitor/services/fetcher/adapters/sender"
	"github.com/fanoxiz/crypto-monitor/services/fetcher/core"
)

func main() {
	reqFrequency := 2 * time.Second
	trackedCoins := []string{"BTC", "ETH"}
	urlToSend := "http://localhost:8081/api/v1/prices"

	openedClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 2 * time.Second,
	}

	exchanges := []core.Exchange{
		api.NewBinanceAdapter(openedClient),
		api.NewBybitAdapter(openedClient),
		api.NewCoinbaseAdapter(openedClient),
		// adapters.NewKrakenAdapter(openedClient), doesn't work in Russia
		api.NewOKXAdapter(openedClient),
	}

	senderAdapter := sender.NewSenderService(openedClient, urlToSend)
	fetcher := core.NewFetcherService(exchanges, senderAdapter)

	log.Println("Fetcher service is running...")
	fetcher.Start(trackedCoins, reqFrequency)
}
