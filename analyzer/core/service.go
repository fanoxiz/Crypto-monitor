package core

import (
	_ "fmt"
	"log"
	"math"
	"sync"

	"github.com/fanoxiz/crypto-monitor/contracts" // allowed core dependency
)

// [Coin][Exchange]Price
type priceCache map[string]map[string]contracts.BidAsk

type AnalyzerService struct {
	fees  map[string]float64
	cache priceCache
	mu    sync.RWMutex
}

func NewAnalyzerService(fees map[string]float64) *AnalyzerService {
	return &AnalyzerService{
		fees:  fees,
		cache: make(priceCache),
	}
}

func (a *AnalyzerService) ProcessPrices(msg contracts.MarketTickerInfo) error {
	a.mu.Lock()

	if _, exists := a.cache[msg.CoinName]; !exists {
		a.cache[msg.CoinName] = make(map[string]contracts.BidAsk)
	}

	for exchangeName, price := range msg.Prices {
		a.cache[msg.CoinName][exchangeName] = price
	}
	a.mu.Unlock()
	a.analyzeCoin(msg.CoinName)
	return nil
}

func (a *AnalyzerService) analyzeCoin(coin string) {
	a.mu.RLock()
	exchangesData := a.cache[coin]
	a.mu.RUnlock()

	if len(exchangesData) < 2 {
		return
	}

	var minAskExchange, maxBidExchange string
	var minAsk = math.MaxFloat64
	var maxBid = 0.0

	for exchName, prices := range exchangesData {
		fee := a.fees[exchName]

		realBuyPrice := prices.Ask * (1.0 + fee)
		realSellPrice := prices.Bid * (1.0 - fee)

		if realBuyPrice < minAsk {
			minAsk = realBuyPrice
			minAskExchange = exchName
		}
		if realSellPrice > maxBid {
			maxBid = realSellPrice
			maxBidExchange = exchName
		}
	}

	var profitAbs float64
	var profitPerc float64

	if minAskExchange != maxBidExchange && minAsk > 0 && maxBid > 0 {
		profitAbs = maxBid - minAsk
		profitPerc = (profitAbs / minAsk) * 100

		if profitPerc > 0 {
			log.Printf("[%s] Куплено на %s (%.2f) | Продано на %s (%.2f) | Профит: +%.3f%% ($%.2f)",
				coin, minAskExchange, minAsk, maxBidExchange, maxBid, profitPerc, profitAbs)
		}
		// TODO: sending to another service
	}
}
