package cache

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/redis/go-redis/v9"
)

type RedisQuoteStore struct {
	client  *redis.Client
	keys    []string
	allowed map[string]bool
}

func NewRedisQuoteStore(client *redis.Client, coins, exchanges []string) *RedisQuoteStore {
	keys := make([]string, 0, len(coins)*len(exchanges))
	allowed := make(map[string]bool, cap(keys))

	for _, coin := range coins {
		for _, exchange := range exchanges {
			key := contracts.QuoteKey(coin, exchange)
			keys = append(keys, key)
			allowed[key] = true
		}
	}

	return &RedisQuoteStore{
		client:  client,
		keys:    keys,
		allowed: allowed,
	}
}

func (s *RedisQuoteStore) Stream(ctx context.Context, handle func(contracts.Quote)) error {
	slog.Info("quote_subscription_starting", "component", "cache", "channel", contracts.QuotesChannel, "configured_keys", len(s.keys))

	pubsub := s.client.Subscribe(ctx, contracts.QuotesChannel)

	defer func() {
		pubsub.Close()
		slog.Info("quote_subscription_closed", "component", "cache", "channel", contracts.QuotesChannel)
	}()

	stop := context.AfterFunc(ctx, func() {
		pubsub.Close()
	})
	defer stop()

	for {
		message, err := pubsub.Receive(ctx)
		if err != nil {
			slog.Debug("quote_receive_failed", "component", "cache", "error", err)

			return err
		}

		switch message := message.(type) {
		case *redis.Subscription:
			if message.Kind != "subscribe" || message.Channel != contracts.QuotesChannel {
				continue
			}

			quotes, err := s.snapshot(ctx)
			if err != nil {
				return err
			}

			slog.Info("quotes_subscribed", "component", "cache", "channel", message.Channel, "snapshot_size", len(quotes))

			for _, quote := range quotes {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				handle(quote)
			}
		case *redis.Message:
			quote, valid := s.decode(message.Payload)
			if valid {
				slog.Debug("quote_event_received", "component", "cache", "coin", quote.Coin, "exchange", quote.Exchange, "invalidated", quote.Invalidated, "age", time.Since(quote.ReceivedAt))

				handle(quote)
			}
		}
	}
}

func (s *RedisQuoteStore) snapshot(ctx context.Context) ([]contracts.Quote, error) {
	startedAt := time.Now()

	values, err := s.client.MGet(ctx, s.keys...).Result()
	if err != nil {
		slog.Debug("quote_snapshot_failed", "component", "cache", "duration", time.Since(startedAt), "error", err)

		return nil, err
	}

	quotes := make([]contracts.Quote, 0, len(values))

	for i, value := range values {
		raw, exists := value.(string)
		if !exists {
			slog.Debug("snapshot_key_missing", "component", "cache", "key", s.keys[i])

			continue
		}

		quote, valid := s.decode(raw)
		if !valid {
			continue
		}

		if contracts.QuoteKey(quote.Coin, quote.Exchange) != s.keys[i] {
			slog.Warn("snapshot_quote_rejected", "component", "cache", "reason", "key_mismatch", "key", s.keys[i], "coin", quote.Coin, "exchange", quote.Exchange)

			continue
		}

		slog.Debug("snapshot_quote_received", "component", "cache", "coin", quote.Coin, "exchange", quote.Exchange, "age", time.Since(quote.ReceivedAt))

		quotes = append(quotes, quote)
	}

	slog.Debug("quote_snapshot_loaded", "component", "cache", "requested_keys", len(s.keys), "quotes", len(quotes), "duration", time.Since(startedAt))

	return quotes, nil
}

func (s *RedisQuoteStore) decode(raw string) (contracts.Quote, bool) {
	var quote contracts.Quote

	if err := json.Unmarshal([]byte(raw), &quote); err != nil {
		slog.Warn("quote_payload_rejected", "component", "cache", "reason", "invalid_json", "bytes", len(raw), "error", err)

		return contracts.Quote{}, false
	}

	allowed := s.allowed[contracts.QuoteKey(quote.Coin, quote.Exchange)]

	if !allowed {
		slog.Debug("quote_payload_rejected", "component", "cache", "reason", "untracked_pair", "coin", quote.Coin, "exchange", quote.Exchange)
	}

	return quote, allowed
}
