package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type BitgetAdapter struct {
	URL string
}

func (BitgetAdapter) Name() string {
	return "Bitget"
}

func (a BitgetAdapter) Stream(ctx context.Context, coins []string, save func(contracts.Quote) error) error {
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

func decodeBitget(raw []byte) (contracts.Quote, error) {
	var data struct {
		Event   string `json:"event"`
		Code    string `json:"code"`
		Message string `json:"msg"`
		Arg     struct {
			Symbol string `json:"symbol"`
		} `json:"arg"`
		Data []struct {
			Bids [][]string `json:"b"`
			Asks [][]string `json:"a"`
		} `json:"data"`
	}

	if err := json.Unmarshal(raw, &data); err != nil {
		return contracts.Quote{}, err
	}

	if data.Event == "error" {
		return contracts.Quote{}, fmt.Errorf("Bitget subscription failed: symbol=%s code=%s message=%s", data.Arg.Symbol, data.Code, data.Message)
	}

	if data.Event == "subscribe" {
		log.Printf("level=INFO component=api event=subscription_confirmed exchange=Bitget symbol=%s", data.Arg.Symbol)

		return contracts.Quote{}, nil
	}

	if len(data.Data) == 0 {
		return contracts.Quote{}, nil
	}

	item := data.Data[0]

	return quoteFromBook(
		strings.TrimSuffix(data.Arg.Symbol, "USDT"),
		"Bitget",
		item.Bids,
		item.Asks,
	), nil
}
