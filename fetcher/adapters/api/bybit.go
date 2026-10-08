package api

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type BybitAdapter struct {
	URL string
}

func (BybitAdapter) Name() string {
	return "Bybit"
}

func (a BybitAdapter) Stream(ctx context.Context, coins []string, save func(contracts.Quote) error) error {
	topics := make([]string, 0, len(coins))

	for _, coin := range coins {
		topics = append(topics, "orderbook.1."+coin+"USDT")
	}

	request := map[string]any{
		"op":   "subscribe",
		"args": topics,
	}

	return stream(ctx, a.URL, request, `{"op":"ping"}`, decodeBybit, save)
}

func decodeBybit(raw []byte) (contracts.Quote, error) {
	var data struct {
		Data struct {
			Symbol string     `json:"s"`
			Bids   [][]string `json:"b"`
			Asks   [][]string `json:"a"`
		} `json:"data"`
	}

	if err := json.Unmarshal(raw, &data); err != nil {
		return contracts.Quote{}, err
	}

	if len(data.Data.Bids) == 0 || len(data.Data.Asks) == 0 || len(data.Data.Bids[0]) < 2 || len(data.Data.Asks[0]) < 2 {
		return contracts.Quote{}, nil
	}

	return contracts.Quote{
		Coin:     strings.TrimSuffix(data.Data.Symbol, "USDT"),
		Exchange: "Bybit",
		BidPrice: data.Data.Bids[0][0],
		BidSize:  data.Data.Bids[0][1],
		AskPrice: data.Data.Asks[0][0],
		AskSize:  data.Data.Asks[0][1],
	}, nil
}
