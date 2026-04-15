package contracts

type TickerMessage struct {
	Coin   string             `json:"coin"`
	Prices map[string]float64 `json:"prices"`
}
