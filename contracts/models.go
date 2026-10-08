package contracts

import "time"

type Quote struct {
	Coin     string `json:"coin"`
	Exchange string `json:"exchange"`

	BidPrice string `json:"bid_price"`
	BidSize  string `json:"bid_size"`
	AskPrice string `json:"ask_price"`
	AskSize  string `json:"ask_size"`

	ReceivedAt time.Time `json:"received_at"`
}

type ProfitDealInfo struct {
	Coin         string `json:"coin"`
	BuyExchange  string `json:"buy_exchange"`
	SellExchange string `json:"sell_exchange"`

	BuyAmountUSDT  float64 `json:"buy_amount_usdt"`
	SellAmountUSDT float64 `json:"sell_amount_usdt"`
	ProfitUSDT     float64 `json:"profit_usdt"`

	DetectedAt time.Time `json:"detected_at"`
}
