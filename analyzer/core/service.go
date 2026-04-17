package core

import (
	"log"
	"math"
	"sync"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts" // allowed core dependency
)

// [Coin][Exchange]Price
type priceCache map[string]map[string]contracts.BidAsk

type AnalyzerService struct {
	fees   map[string]float64
	cache  priceCache
	mu     sync.RWMutex
	sender DealSender
}

func NewAnalyzerService(fees map[string]float64, sender DealSender) *AnalyzerService {
	return &AnalyzerService{
		fees:   fees,
		cache:  make(priceCache),
		sender: sender,
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
			deal := contracts.ProfitDealInfo{
				CoinName:      coin,
				AskExchange:   minAskExchange,
				BifExchange:   maxBidExchange,
				AskPrice:      minAsk,
				BidPrice:      maxBid,
				ProfitAbs:     profitAbs,
				ProfitPercent: profitPerc,
				Timestamp:     time.Now().UTC(),
			}

			if err := a.sender.Send(deal); err != nil {
				log.Printf("Ошибка отправки сделки в Executor: %v", err)
			}
		}
	}
}
