package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type BybitAdapter struct {
	URL string
}

func (BybitAdapter) Name() string {
	return "Bybit"
}

func (a BybitAdapter) Stream(ctx context.Context, coins []string, save func(contracts.Quote) error, onHealthy func()) error {
	topics := make([]string, 0, len(coins))

	for _, coin := range coins {
		topics = append(topics, "orderbook.1."+coin+"USDT")
	}

	request := map[string]any{
		"op":     "subscribe",
		"args":   topics,
		"req_id": "quotes",
	}

	return stream(ctx, a.Name(), a.URL, coins, request, `{"op":"ping","req_id":"heartbeat"}`, decodeBybit, save, onHealthy)
}

func decodeBybit(raw []byte) (streamMessage, error) {
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
		return streamMessage{}, err
	}

	if data.Success != nil && !*data.Success {
		return streamMessage{}, fmt.Errorf("%w: Bybit request failed: operation=%s request_id=%s message=%s", errRequestRejected, data.Operation, data.RequestID, data.Message)
	}

	if data.Operation == "ping" && data.RequestID == heartbeatPayload && data.Message == "pong" && data.Success != nil {
		return streamMessage{
			heartbeat: true,
		}, nil
	}

	if data.Operation == "subscribe" && data.Success != nil {
		return streamMessage{
			confirmedAll: data.RequestID == "quotes",
		}, nil
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

	return streamMessage{
		quote:    quote,
		dataCoin: strings.TrimSuffix(data.Data.Symbol, "USDT"),
	}, nil
}
