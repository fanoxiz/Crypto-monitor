package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
		return contracts.Quote{}, err
	}

	if data.Code != nil {
		return contracts.Quote{}, fmt.Errorf("Binance subscription failed: code=%d message=%s", *data.Code, data.Message)
	}

	if string(data.Result) == "null" {
		log.Printf("level=INFO component=api event=subscription_confirmed exchange=Binance request_id=%s", data.ID)

		return contracts.Quote{}, nil
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
