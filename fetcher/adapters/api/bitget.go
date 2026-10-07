package api

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/fanoxiz/crypto-monitor/fetcher/core"
)

type BitgetAdapter struct {
	URL string
}

func (BitgetAdapter) Name() string {
	return "Bitget"
}

func (a BitgetAdapter) Stream(ctx context.Context, coins []string, save func(core.Quote) error) error {
	args := make([]map[string]string, 0, len(coins))

	for _, coin := range coins {
		args = append(args, map[string]string{
			"instType": "spot",
			"topic":    "books1",
			"symbol":   coin + "USDT",
		})
	}

	request := map[string]any{
		"op":   "subscribe",
		"args": args,
	}

	return stream(ctx, a.URL, request, "ping", decodeBitget, save)
}

func decodeBitget(raw []byte) (core.Quote, error) {
	var data struct {
		Arg struct {
			Symbol string `json:"symbol"`
		} `json:"arg"`
		Data []struct {
			Bids [][]string `json:"b"`
			Asks [][]string `json:"a"`
		} `json:"data"`
	}

	if err := json.Unmarshal(raw, &data); err != nil {
		return core.Quote{}, err
	}

	if len(data.Data) == 0 {
		return core.Quote{}, nil
	}

	item := data.Data[0]

	if len(item.Bids) == 0 || len(item.Asks) == 0 || len(item.Bids[0]) < 2 || len(item.Asks[0]) < 2 {
		return core.Quote{}, nil
	}

	return core.Quote{
		Coin:     strings.TrimSuffix(data.Arg.Symbol, "USDT"),
		Exchange: "Bitget",
		BidPrice: item.Bids[0][0],
		BidSize:  item.Bids[0][1],
		AskPrice: item.Asks[0][0],
		AskSize:  item.Asks[0][1],
	}, nil
}
