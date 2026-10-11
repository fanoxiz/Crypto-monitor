package api

import (
	"context"
	"log/slog"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type streamMessage struct {
	quote         contracts.Quote
	dataCoin      string
	confirmedCoin string
	confirmedAll  bool
	heartbeat     bool
}

type instrumentStatus struct {
	confirmed   bool
	lastDataAt  time.Time
	lastQuoteAt time.Time
	invalidated bool
}

func recordMessage(logger *slog.Logger, instruments map[string]*instrumentStatus, message streamMessage, receivedAt time.Time) {
	if message.confirmedAll {
		for coin, status := range instruments {
			if !status.confirmed {
				status.confirmed = true
				logger.Info("subscription_confirmed", "coin", coin)
			}
		}
	}

	if message.confirmedCoin != "" {
		status, exists := instruments[message.confirmedCoin]
		if !exists {
			logger.Warn("subscription_unexpected", "coin", message.confirmedCoin)
		} else if !status.confirmed {
			status.confirmed = true
			logger.Info("subscription_confirmed", "coin", message.confirmedCoin)
		}
	}

	if message.dataCoin == "" {
		return
	}

	status, exists := instruments[message.dataCoin]
	if !exists {
		logger.Warn("market_data_unexpected", "coin", message.dataCoin)

		return
	}

	if status.lastDataAt.IsZero() {
		logger.Info("subscription_data_observed", "coin", message.dataCoin, "confirmed", status.confirmed)
	}

	status.lastDataAt = receivedAt

	if message.quote.Coin != "" {
		status.lastQuoteAt = receivedAt
		status.invalidated = message.quote.Invalidated
	}
}

func logInstrumentStatus(ctx context.Context, logger *slog.Logger, instruments map[string]*instrumentStatus, coins []string, reason string) {
	now := time.Now().UTC()

	for _, coin := range coins {
		status := instruments[coin]
		level := slog.LevelInfo
		if !status.confirmed || status.lastQuoteAt.IsZero() || status.invalidated {
			level = slog.LevelWarn
		}

		attrs := []any{
			"coin", coin,
			"reason", reason,
			"confirmed", status.confirmed,
			"data_received", !status.lastDataAt.IsZero(),
			"quote_received", !status.lastQuoteAt.IsZero(),
			"last_data_at", status.lastDataAt,
			"last_quote_at", status.lastQuoteAt,
			"invalidated", status.invalidated,
		}

		if !status.lastDataAt.IsZero() {
			attrs = append(attrs, "data_age", now.Sub(status.lastDataAt))
		}

		if !status.lastQuoteAt.IsZero() {
			attrs = append(attrs, "quote_age", now.Sub(status.lastQuoteAt))
		}

		logger.Log(ctx, level, "instrument_status", attrs...)
	}
}
