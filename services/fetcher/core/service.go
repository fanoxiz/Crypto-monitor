package core

import (
	"log"
	"sync"
	"time"

	"github.com/fanoxiz/crypto-monitor/services/contracts" // allowed core dependency
)

type FetcherService struct {
	exchanges []Exchange
	sender    SenderToAnalyzer
}

func NewFetcherService(exchanges []Exchange, sender SenderToAnalyzer) *FetcherService {
	return &FetcherService{
		exchanges: exchanges,
		sender:    sender,
	}
}

func (s *FetcherService) Start(coins []string, freq time.Duration) {
	ticker := time.NewTicker(freq)
	defer ticker.Stop()

	for {
		<-ticker.C
		for _, coin := range coins {
			// TODO: parallel collection
			s.CollectPrices(coin)
		}
	}
}

func (s *FetcherService) CollectPrices(coinName string) {
	var wg sync.WaitGroup
	var mu sync.Mutex

	prices := make(map[string]float64)

	for _, ex := range s.exchanges {
		wg.Go(func() {
			price, err := ex.GetPrice(coinName)
			if err != nil {
				log.Printf("[%s] Ошибка на %s: %v", coinName, ex.GetName(), err)
				return
			}

			mu.Lock()
			defer mu.Unlock()
			prices[ex.GetName()] = price
		})
	}

	wg.Wait()
	if len(prices) < 2 {
		return
	}

	msg := contracts.TickerMessage{
		Coin:   coinName,
		Prices: prices,
	}

	if err := s.sender.Send(msg); err != nil {
		log.Printf("Ошибка при отправке данных по %s: %v", coinName, err)
	}
}
