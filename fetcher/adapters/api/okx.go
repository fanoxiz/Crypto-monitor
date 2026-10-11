package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type OKXAdapter struct {
	URL string
}

func (OKXAdapter) Name() string {
	return "OKX"
}

func (a OKXAdapter) Stream(ctx context.Context, coins []string, save func(contracts.Quote) error, onHealthy func()) error {
	args := make([]map[string]string, 0, len(coins))

	for _, coin := range coins {
		args = append(args, map[string]string{
			"channel": "bbo-tbt",
			"instId":  coin + "-USDT",
		})
	}

	request := map[string]any{
		"op":   "subscribe",
		"args": args,
	}

	return stream(ctx, a.Name(), a.URL, coins, request, "ping", decodeOKX, save, onHealthy)
}

func decodeOKX(raw []byte) (streamMessage, error) {
	var data struct {
		Event   string `json:"event"`
		Code    string `json:"code"`
		Message string `json:"msg"`
		Arg     struct {
			Symbol  string `json:"instId"`
			Channel string `json:"channel"`
		} `json:"arg"`
		Data []struct {
			Bids      [][]string  `json:"bids"`
			Asks      [][]string  `json:"asks"`
			Timestamp json.Number `json:"ts"`
		} `json:"data"`
	}

	if err := json.Unmarshal(raw, &data); err != nil {
		return streamMessage{}, err
	}

	if data.Event == "error" {
		return streamMessage{}, fmt.Errorf("%w: OKX subscription failed: symbol=%s code=%s message=%s", errRequestRejected, data.Arg.Symbol, data.Code, data.Message)
	}

	if data.Event == "subscribe" {
		if data.Arg.Channel == "bbo-tbt" {
			return streamMessage{
				confirmedCoin: strings.TrimSuffix(data.Arg.Symbol, "-USDT"),
			}, nil
		}

		return streamMessage{}, nil
	}

	if len(data.Data) == 0 {
		return streamMessage{}, nil
	}

	item := data.Data[0]

	quote := quoteFromBook(
		strings.TrimSuffix(data.Arg.Symbol, "-USDT"),
		"OKX",
		item.Bids,
		item.Asks,
	)

	if quote.Coin != "" {
		quote.SourceAt = sourceTime(item.Timestamp)
	}

	return streamMessage{
		quote:    quote,
		dataCoin: strings.TrimSuffix(data.Arg.Symbol, "-USDT"),
	}, nil
}
