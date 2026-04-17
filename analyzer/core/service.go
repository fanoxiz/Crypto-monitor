package core

import (
	"log"
	"math"
	"sync"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts" // allowed core dependency
)

type coinPriceState struct {
	mu     sync.RWMutex
	prices map[string]contracts.BidAsk
}

// [Coin]StateWithOwnMutex
type priceCache map[string]*coinPriceState

type AnalyzerService struct {
	fees    map[string]float64
	cache   priceCache
	cacheMu sync.RWMutex
	sender  DealSender
}

func NewAnalyzerService(fees map[string]float64, sender DealSender) *AnalyzerService {
	return &AnalyzerService{
		fees:   fees,
		cache:  make(priceCache),
		sender: sender,
	}
}

func (a *AnalyzerService) ProcessPrices(msg contracts.MarketTickerInfo) error {
	coinState := a.getOrCreateCoinState(msg.CoinName)

	coinState.mu.Lock()
	coinState.prices[msg.ExchangeName] = msg.Price
	coinState.mu.Unlock()

	a.analyzeCoin(msg.CoinName)
	return nil
}

func (a *AnalyzerService) analyzeCoin(coin string) {
	coinState, exists := a.getCoinState(coin)
	if !exists {
		return
	}

	coinState.mu.RLock()
	if len(coinState.prices) < 2 {
		coinState.mu.RUnlock()
		return
	}

	exchangesData := make(map[string]contracts.BidAsk, len(coinState.prices))
	for exchangeName, price := range coinState.prices {
		exchangesData[exchangeName] = price
	}
	coinState.mu.RUnlock()

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
				BidExchange:   maxBidExchange,
				AskPrice:      minAsk,
				BidPrice:      maxBid,
				ProfitAbs:     profitAbs,
				ProfitPercent: profitPerc,
				Timestamp:     time.Now().UTC(),
			}

			if err := a.sender.Send(deal); err != nil {
				log.Printf("Ошибка отправки сделки: %v", err)
			}
		}
	}
}

func (a *AnalyzerService) getCoinState(coin string) (*coinPriceState, bool) {
	a.cacheMu.RLock()
	coinState, exists := a.cache[coin]
	a.cacheMu.RUnlock()

	return coinState, exists
}

func (a *AnalyzerService) getOrCreateCoinState(coin string) *coinPriceState {
	if coinState, exists := a.getCoinState(coin); exists {
		return coinState
	}

	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()

	if coinState, exists := a.cache[coin]; exists {
		return coinState
	}

	coinState := &coinPriceState{prices: make(map[string]contracts.BidAsk)}
	a.cache[coin] = coinState

	return coinState
}
