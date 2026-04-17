package core

import (
	"log"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type FetcherService struct {
	exchanges  []ExchangeAdapter
	sender     PriceSender
	streamChan chan contracts.MarketTickerInfo
}

func NewFetcherService(exchanges []ExchangeAdapter, sender PriceSender) *FetcherService {
	return &FetcherService{
		exchanges:  exchanges,
		sender:     sender,
		streamChan: make(chan contracts.MarketTickerInfo, 100),
	}
}

func (s *FetcherService) Start(coins []string, freq time.Duration) {
	go s.senderWorker()

	ticker := time.NewTicker(freq)
	defer ticker.Stop()

	for {
		<-ticker.C
		for _, coin := range coins {
			for _, ex := range s.exchanges {
				go s.fetchSingle(coin, ex)
			}
		}
	}
}

func (s *FetcherService) fetchSingle(coin string, ex ExchangeAdapter) {
	price, err := ex.GetPrice(coin)
	if err != nil {
		log.Printf("[%s] Ошибка на %s: %v", coin, ex.GetName(), err)
		return
	}

	msg := contracts.MarketTickerInfo{
		CoinName: coin,
		Prices: map[string]contracts.BidAsk{
			ex.GetName(): price,
		},
	}

	s.streamChan <- msg
}

func (s *FetcherService) senderWorker() {
	for msg := range s.streamChan {
		if err := s.sender.Send(msg); err != nil {
			log.Printf("Ошибка при отправке в Analyzer: %v", err)
		}
	}
}
