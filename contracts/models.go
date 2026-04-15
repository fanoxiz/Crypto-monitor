package contracts

type MarketTickerInfo struct {
	Coin   string             `json:"coin"`
	Prices map[string]float64 `json:"prices"`
}
