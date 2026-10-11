package api

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/gorilla/websocket"
)

func stream(ctx context.Context, exchange, endpoint string, coins []string, request any, heartbeat string, decode func([]byte) (streamMessage, error), save func(contracts.Quote) error) error {
	logger := slog.With("component", "api", "exchange", exchange)
	logger.Debug("connection_opening")

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return err
	}

	startedAt := time.Now()
	summaryAt := startedAt
	var lastQuoteAt time.Time
	var messages, quotes, invalidations, ignored int
	instruments := make(map[string]*instrumentStatus, len(coins))

	for _, coin := range coins {
		instruments[coin] = &instrumentStatus{}
	}

	logger.Info("connection_opened")

	defer func() {
		conn.Close()
		logger.Info("connection_closed", "duration", time.Since(startedAt), "messages", messages, "quotes", quotes, "invalidations", invalidations, "ignored", ignored, "last_quote_at", lastQuoteAt)
		logInstrumentStatus(ctx, logger, instruments, coins, "connection_closed")
	}()

	if err := conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		return err
	}

	if err := conn.WriteJSON(request); err != nil {
		return err
	}

	logger.Debug("subscription_sent", "request", request)

	quoteDeadline := time.Now().Add(500 * time.Millisecond)

	if err := conn.SetReadDeadline(quoteDeadline); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})

	defer func() {
		cancel()
		conn.Close()
		<-heartbeatDone
	}()

	go func() {
		defer close(heartbeatDone)

		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Debug("heartbeat_stopped")

				conn.Close()
				return
			case <-ticker.C:
				if heartbeat == "" {
					continue
				}

				conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

				if err := conn.WriteMessage(websocket.TextMessage, []byte(heartbeat)); err != nil {
					logger.Debug("heartbeat_write_failed", "error", err)

					conn.Close()
					return
				}

				logger.Debug("heartbeat_sent")
			}
		}
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			logger.Debug("message_read_failed", "quote_deadline", quoteDeadline, "error", err)

			return err
		}

		receivedAt := time.Now().UTC()
		messages++

		logger.Debug("message_received", "bytes", len(data), "received_at", receivedAt)

		if receivedAt.After(quoteDeadline) {
			logger.Debug("quote_timeout", "quote_deadline", quoteDeadline)

			return fmt.Errorf("no quote received within 500ms")
		}

		if string(data) == "pong" {
			logger.Debug("heartbeat_received")

			continue
		}

		message, err := decode(data)
		if err != nil {
			logger.Debug("message_decode_failed", "error", err)

			return err
		}

		recordMessage(logger, instruments, message, receivedAt)
		quote := message.quote

		if quote.Coin == "" {
			ignored++
			logger.Debug("message_ignored", "reason", "no_quote")

			continue
		}

		quote.ReceivedAt = receivedAt
		lastQuoteAt = receivedAt
		quotes++

		if quote.Invalidated {
			invalidations++
			logger.Info("quote_invalidated", "coin", quote.Coin, "reason", "empty_book", "received_at", receivedAt)
		} else {
			logger.Debug("quote_received", "coin", quote.Coin, "bid_price", quote.BidPrice, "bid_size", quote.BidSize, "ask_price", quote.AskPrice, "ask_size", quote.AskSize, "source_at", quote.SourceAt, "received_at", receivedAt)
		}

		if receivedAt.Sub(summaryAt) >= 30*time.Second {
			logger.Info("stream_summary", "duration", receivedAt.Sub(startedAt), "messages", messages, "quotes", quotes, "invalidations", invalidations, "ignored", ignored, "last_quote_at", lastQuoteAt)
			logInstrumentStatus(ctx, logger, instruments, coins, "stream_summary")
			summaryAt = receivedAt
		}

		quoteDeadline = receivedAt.Add(500 * time.Millisecond)

		if err := conn.SetReadDeadline(quoteDeadline); err != nil {
			return err
		}

		if err := save(quote); err != nil {
			logger.Debug("quote_callback_failed", "coin", quote.Coin, "error", err)

			return err
		}
	}
}
