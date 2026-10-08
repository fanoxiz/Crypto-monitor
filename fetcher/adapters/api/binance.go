package api

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type BinanceAdapter struct {
	URL string
}

func (BinanceAdapter) Name() string {
	return "Binance"
}

func (a BinanceAdapter) Stream(ctx context.Context, coins []string, save func(contracts.Quote) error) error {
	topics := make([]string, 0, len(coins))

	for _, coin := range coins {
		topics = append(topics, strings.ToLower(coin)+"usdt@bookTicker")
	}

	request := map[string]any{
		"method": "SUBSCRIBE",
		"params": topics,
		"id":     1,
	}

	return stream(ctx, a.URL, request, "", decodeBinance, save)
}

func decodeBinance(raw []byte) (contracts.Quote, error) {
	var data struct {
		Symbol   string `json:"s"`
		BidPrice string `json:"b"`
		BidSize  string `json:"B"`
		AskPrice string `json:"a"`
		AskSize  string `json:"A"`
	}

	if err := json.Unmarshal(raw, &data); err != nil {
		return contracts.Quote{}, err
	}

	if data.Symbol == "" {
		return contracts.Quote{}, nil
	}

	return contracts.Quote{
		Coin:     strings.TrimSuffix(data.Symbol, "USDT"),
		Exchange: "Binance",
		BidPrice: data.BidPrice,
		BidSize:  data.BidSize,
		AskPrice: data.AskPrice,
		AskSize:  data.AskSize,
	}, nil
}
