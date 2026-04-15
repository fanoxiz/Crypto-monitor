package contracts

type BidAsk struct {
	Bid float64
	Ask float64
}

type MarketTickerInfo struct {
	Coin   string            `json:"coin"`
	Prices map[string]BidAsk `json:"prices"`
}
