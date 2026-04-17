package core

import (
	"github.com/fanoxiz/crypto-monitor/contracts"
)

type ExchangeAdapter interface {
	GetName() string
	GetPrice(symbol string) (contracts.BidAsk, error)
}

type PriceSender interface {
	Send(msg contracts.MarketTickerInfo) error
}
