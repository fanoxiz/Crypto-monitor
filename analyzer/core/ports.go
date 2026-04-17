package core

import "github.com/fanoxiz/crypto-monitor/contracts"

type Analyzer interface {
	ProcessPrices(msg contracts.MarketTickerInfo) error
}

type DealSender interface {
	Send(msg contracts.ProfitDealInfo) error
}
