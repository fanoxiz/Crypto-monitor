package api

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/gorilla/websocket"
)

type streamMessage struct {
	quote         contracts.Quote
	dataCoin      string
	confirmedCoin string
	confirmedAll  bool
	heartbeat     bool
}

var errRequestRejected = errors.New("exchange request rejected")

func stream(ctx context.Context, exchange, endpoint string, coins []string, request any, heartbeat string, decode func([]byte) (streamMessage, error), save func(contracts.Quote) error, onHealthy func()) error {
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

	if err := conn.SetWriteDeadline(time.Now().Add(heartbeatTimeout)); err != nil {
		return err
	}

	if err := conn.WriteJSON(request); err != nil {
		return err
	}

	logger.Debug("subscription_sent", "request", request)

	summarize := func(at time.Time) {
		if at.Sub(summaryAt) < 30*time.Second {
			return
		}

		logger.Info("stream_summary", "duration", at.Sub(startedAt), "messages", messages, "quotes", quotes, "invalidations", invalidations, "ignored", ignored, "last_quote_at", lastQuoteAt)
		logInstrumentStatus(ctx, logger, instruments, coins, "stream_summary")
		summaryAt = at
	}

	ctx, cancel := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	heartbeats := make(chan time.Time, 1)
	heartbeatErrors := make(chan error, 1)

	onPong := func(at time.Time) {
		select {
		case heartbeats <- at:
		default:
		}

		logger.Debug("heartbeat_received", "received_at", at.UTC())
		summarize(at)
	}

	conn.SetPongHandler(func(payload string) error {
		if heartbeat == "" && payload == heartbeatPayload {
			onPong(time.Now())
		}

		return nil
	})

	defer func() {
		cancel()
		conn.Close()
		<-heartbeatDone
	}()

	go func() {
		defer close(heartbeatDone)
		defer conn.Close()

		runHeartbeat(ctx, conn, logger, heartbeat, heartbeats, heartbeatErrors, onHealthy)
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			select {
			case heartbeatErr := <-heartbeatErrors:
				err = heartbeatErr
			default:
			}

			logger.Debug("message_read_failed", "error", err)

			return err
		}

		receivedAt := time.Now().UTC()
		messages++

		logger.Debug("message_received", "bytes", len(data), "received_at", receivedAt)

		if heartbeat == "ping" && string(data) == "pong" {
			onPong(time.Now())

			continue
		}

		message, err := decode(data)
		if err != nil {
			if errors.Is(err, errRequestRejected) {
				return err
			}

			ignored++
			logger.Warn("message_decode_failed", "reason", "invalid_payload", "error", err)
			summarize(receivedAt)

			continue
		}

		if message.heartbeat {
			onPong(time.Now())

			continue
		}

		recordMessage(logger, instruments, message, receivedAt)
		quote := message.quote

		if quote.Coin == "" {
			ignored++
			logger.Debug("message_ignored", "reason", "no_quote")
			summarize(receivedAt)

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

		summarize(receivedAt)

		if err := save(quote); err != nil {
			logger.Debug("quote_callback_failed", "coin", quote.Coin, "error", err)

			return err
		}
	}
}
