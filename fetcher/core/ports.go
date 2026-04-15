package core

import (
	"github.com/fanoxiz/crypto-monitor/contracts" // allowed core dependency
)

type ExchangeAdapter interface {
	GetName() string
	GetPrice(symbol string) (float64, error)
}

type Sender interface {
	Send(msg contracts.MarketTickerInfo) error
}
