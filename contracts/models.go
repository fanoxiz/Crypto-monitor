package contracts

import "time"

type BidAsk struct {
	Bid float64
	Ask float64
}

type MarketTickerInfo struct {
	CoinName     string `json:"coin"`
	ExchangeName string `json:"exchange"`
	Price        BidAsk `json:"price"`
}

type ProfitDealInfo struct {
	CoinName      string    `json:"coin"`
	AskExchange   string    `json:"buy_exchange"`
	BidExchange   string    `json:"sell_exchange"`
	AskPrice      float64   `json:"buy_price"`
	BidPrice      float64   `json:"sell_price"`
	ProfitAbs     float64   `json:"profit_abs"`
	ProfitPercent float64   `json:"profit_percent"`
	Timestamp     time.Time `json:"timestamp"`
}
