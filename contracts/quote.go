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
