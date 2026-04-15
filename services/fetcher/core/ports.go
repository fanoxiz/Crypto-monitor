package core

import "github.com/fanoxiz/crypto-monitor/services/contracts" // allowed core dependency

type Exchange interface {
	GetName() string
	GetPrice(symbol string) (float64, error)
}

type SenderToAnalyzer interface {
	Send(msg contracts.TickerMessage) error
}
