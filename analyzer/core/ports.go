package core

import (
	"github.com/fanoxiz/crypto-monitor/contracts"
)

type Analyzer interface {
	ProcessPrices(msg contracts.MarketTickerInfo) error
}

type DealSender interface {
	Send(msg contracts.ProfitDealInfo) error
}

type PriceStore interface {
	SetPrice(coin, exchange string, price contracts.BidAsk) error
	GetPrices(coin string) (map[string]contracts.BidAsk, error)
}
