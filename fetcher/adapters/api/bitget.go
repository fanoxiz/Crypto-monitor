package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type BitgetAdapter struct {
	URL string
}

func (BitgetAdapter) Name() string {
	return "Bitget"
}

func (a BitgetAdapter) Stream(ctx context.Context, coins []string, save func(contracts.Quote) error, onHealthy func()) error {
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

	return stream(ctx, a.Name(), a.URL, coins, request, "ping", decodeBitget, save, onHealthy)
}

func decodeBitget(raw []byte) (streamMessage, error) {
	var data struct {
		Event     string      `json:"event"`
		Code      string      `json:"code"`
		Message   string      `json:"msg"`
		Timestamp json.Number `json:"ts"`
		Arg       struct {
			Symbol   string `json:"symbol"`
			Topic    string `json:"topic"`
			InstType string `json:"instType"`
		} `json:"arg"`
		Data []struct {
			Bids      [][]string  `json:"b"`
			Asks      [][]string  `json:"a"`
			Timestamp json.Number `json:"ts"`
		} `json:"data"`
	}

	if err := json.Unmarshal(raw, &data); err != nil {
		return streamMessage{}, err
	}

	if data.Event == "error" {
		return streamMessage{}, fmt.Errorf("%w: Bitget subscription failed: symbol=%s code=%s message=%s", errRequestRejected, data.Arg.Symbol, data.Code, data.Message)
	}

	if data.Event == "subscribe" {
		if data.Arg.Topic == "books1" && data.Arg.InstType == "spot" {
			return streamMessage{
				confirmedCoin: strings.TrimSuffix(data.Arg.Symbol, "USDT"),
			}, nil
		}

		return streamMessage{}, nil
	}

	if len(data.Data) == 0 {
		return streamMessage{}, nil
	}

	item := data.Data[0]

	quote := quoteFromBook(
		strings.TrimSuffix(data.Arg.Symbol, "USDT"),
		"Bitget",
		item.Bids,
		item.Asks,
	)

	if quote.Coin != "" {
		quote.SourceAt = sourceTime(item.Timestamp)
		if quote.SourceAt.IsZero() {
			quote.SourceAt = sourceTime(data.Timestamp)
		}
	}

	return streamMessage{
		quote:    quote,
		dataCoin: strings.TrimSuffix(data.Arg.Symbol, "USDT"),
	}, nil
}
