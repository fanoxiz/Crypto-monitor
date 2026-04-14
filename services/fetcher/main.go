package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/fanoxiz/crypto-monitor/services/fetcher/adapters"
	"github.com/fanoxiz/crypto-monitor/services/fetcher/core"
)

var reqFrequency = 500 * time.Millisecond

// type StringFloat64Map struct {
//   m sync.Map
// }

// func (s *StringFloat64Map) Store(key string, val float64) {
//   s.m.Store(key, val)
// }

// func (s *StringFloat64Map) Load(key string) (float64, bool) {
// 	v, ok := s.m.Load(key)
// 	if ok {
// 		return v.(float64), true
// 	}
// 	return 0, false
// }

func main() {
	openedClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
		},
		Timeout: 1 * time.Second,
	}

	exchanges := []core.Exchange{
		adapters.NewBinanceAdapter(openedClient),
		adapters.NewBybitAdapter(openedClient),
		adapters.NewCoinbaseAdapter(openedClient),
		// adapters.NewKrakenAdapter(openedClient), doesn't work in Russia
		adapters.NewOKXAdapter(openedClient),
	}

	trackedCoins := []string{"BTC", "ETH"}

	log.Println("Starting fetcher service...")

	var wg sync.WaitGroup
	for {
		wg.Add(len(trackedCoins))
		for _, coin := range trackedCoins {
			go func() {
				price, exchange := trackBestPrice(coin, exchanges)
				fmt.Printf("[%s] [$%.2f \t\t(%s)]\n", coin, price, exchange)
				wg.Done()
			}()
		}
		wg.Wait()
		fmt.Printf("------------\n")
		time.Sleep(reqFrequency)
	}
}

func trackBestPrice(coinName string, exchanges []core.Exchange) (float64, string) {
	var wg sync.WaitGroup

	var mu sync.Mutex
	var minToBuy = math.MaxFloat64
	var bestExchange = "-"

	for _, ex := range exchanges {
		wg.Go(func() { // since Go 1.25
			price, err := ex.GetPrice(coinName)
			if err != nil {
				log.Printf("Ошибка на %s: %v", ex.GetName(), err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if price < minToBuy {
				minToBuy = price
				bestExchange = ex.GetName()
			}
		})
	}
	wg.Wait()
	return minToBuy, bestExchange
}
