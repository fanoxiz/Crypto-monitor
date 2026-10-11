package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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

	return stream(ctx, a.Name(), a.URL, request, `{"op":"ping"}`, decodeBybit, save)
}

func decodeBybit(raw []byte) (contracts.Quote, error) {
	var data struct {
		Success         *bool       `json:"success"`
		Operation       string      `json:"op"`
		Message         string      `json:"ret_msg"`
		RequestID       string      `json:"req_id"`
		Timestamp       json.Number `json:"ts"`
		EngineTimestamp json.Number `json:"cts"`
		Data            struct {
			Symbol string     `json:"s"`
			Bids   [][]string `json:"b"`
			Asks   [][]string `json:"a"`
		} `json:"data"`
	}

	if err := json.Unmarshal(raw, &data); err != nil {
		return contracts.Quote{}, err
	}

	if data.Success != nil && !*data.Success {
		return contracts.Quote{}, fmt.Errorf("Bybit request failed: operation=%s request_id=%s message=%s", data.Operation, data.RequestID, data.Message)
	}

	if data.Operation == "subscribe" && data.Success != nil {
		slog.Info("subscription_confirmed", "component", "api", "exchange", "Bybit", "request_id", data.RequestID)

		return contracts.Quote{}, nil
	}

	quote := quoteFromBook(
		strings.TrimSuffix(data.Data.Symbol, "USDT"),
		"Bybit",
		data.Data.Bids,
		data.Data.Asks,
	)

	if quote.Coin != "" {
		quote.SourceAt = sourceTime(data.EngineTimestamp)
		if quote.SourceAt.IsZero() {
			quote.SourceAt = sourceTime(data.Timestamp)
		}
	}

	return quote, nil
}
