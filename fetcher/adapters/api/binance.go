package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type BinanceAdapter struct {
	URL string
}

func (BinanceAdapter) Name() string {
	return "Binance"
}

func (a BinanceAdapter) Stream(ctx context.Context, coins []string, save func(contracts.Quote) error, onHealthy func()) error {
	topics := make([]string, 0, len(coins))

	for _, coin := range coins {
		topics = append(topics, strings.ToLower(coin)+"usdt@bookTicker")
	}

	request := map[string]any{
		"method": "SUBSCRIBE",
		"params": topics,
		"id":     1,
	}

	return stream(ctx, a.Name(), a.URL, coins, request, "", decodeBinance, save, onHealthy)
}

func decodeBinance(raw []byte) (streamMessage, error) {
	var data struct {
		Code     *int            `json:"code"`
		Message  string          `json:"msg"`
		ID       json.RawMessage `json:"id"`
		Result   json.RawMessage `json:"result"`
		Symbol   string          `json:"s"`
		BidPrice string          `json:"b"`
		BidSize  string          `json:"B"`
		AskPrice string          `json:"a"`
		AskSize  string          `json:"A"`
	}

	if err := json.Unmarshal(raw, &data); err != nil {
		return streamMessage{}, err
	}

	if data.Code != nil {
		return streamMessage{}, fmt.Errorf("%w: Binance subscription failed: code=%d message=%s", errRequestRejected, *data.Code, data.Message)
	}

	if string(data.Result) == "null" {
		return streamMessage{
			confirmedAll: string(data.ID) == "1",
		}, nil
	}

	if data.Symbol == "" {
		return streamMessage{}, nil
	}

	coin := strings.TrimSuffix(data.Symbol, "USDT")

	return streamMessage{
		dataCoin: coin,
		quote: contracts.Quote{
			Coin:     coin,
			Exchange: "Binance",
			BidPrice: data.BidPrice,
			BidSize:  data.BidSize,
			AskPrice: data.AskPrice,
			AskSize:  data.AskSize,
		},
	}, nil
}
