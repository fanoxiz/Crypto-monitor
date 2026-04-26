package core

import (
	"log"
	"math"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts" // allowed core dependency
)

type AnalyzerService struct {
	fees   map[string]float64
	store  PriceStore
	sender DealSender
}

func NewAnalyzerService(fees map[string]float64, sender DealSender, store PriceStore) *AnalyzerService {
	return &AnalyzerService{
		fees:   fees,
		store:  store,
		sender: sender,
	}
}

func (a *AnalyzerService) ProcessPrices(msg contracts.MarketTickerInfo) error {
	if err := a.store.SetPrice(msg.CoinName, msg.ExchangeName, msg.Price); err != nil {
		return err
	}

	a.analyzeCoin(msg.CoinName)
	return nil
}

func (a *AnalyzerService) analyzeCoin(coin string) {
	exchangesData, err := a.store.GetPrices(coin)
	if err != nil {
		log.Printf("Ошибка чтения кэша цен: %v", err)
		return
	}

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
