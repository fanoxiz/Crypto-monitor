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
	client *redis.Client
	ttl    time.Duration
}

func NewRedisQuoteStore(client *redis.Client, ttl time.Duration) *RedisQuoteStore {
	return &RedisQuoteStore{
		client: client,
		ttl:    ttl,
	}
}

func (s *RedisQuoteStore) SaveBatch(ctx context.Context, quotes []contracts.Quote) error {
	var saved, invalidated, skipped int

	_, err := s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, quote := range quotes {
			now := time.Now()
			age := now.Sub(quote.ReceivedAt)

			if quote.ReceivedAt.IsZero() || age < 0 || age >= s.ttl {
				skipped++
				slog.Debug("quote_write_skipped", "component", "cache", "coin", quote.Coin, "exchange", quote.Exchange, "reason", "invalid_received_at", "age", age, "invalidated", quote.Invalidated)

				continue
			}

			base := quote.SourceAt
			lifetime := s.ttl

			if quote.Invalidated {
				base = quote.ReceivedAt
			} else if quote.Exchange == "Binance" {
				base = quote.ReceivedAt
				lifetime /= 2
			}

			if base.IsZero() || base.After(now) {
				skipped++
				slog.Debug("quote_write_skipped", "component", "cache", "coin", quote.Coin, "exchange", quote.Exchange, "reason", "invalid_timestamp", "source_at", quote.SourceAt, "received_at", quote.ReceivedAt)

				continue
			}

			quote.ExpiresAt = time.UnixMilli(base.Add(lifetime).UnixMilli()).UTC()
			ttl := quote.ExpiresAt.Sub(now)

			if ttl <= 0 {
				skipped++
				slog.Debug("quote_write_skipped", "component", "cache", "coin", quote.Coin, "exchange", quote.Exchange, "reason", "expired", "source_at", quote.SourceAt, "received_at", quote.ReceivedAt, "expires_at", quote.ExpiresAt, "remaining_ttl", ttl)

				continue
			}

			data, err := json.Marshal(quote)
			if err != nil {
				return err
			}

			key := contracts.QuoteKey(quote.Coin, quote.Exchange)

			if quote.Invalidated {
				pipe.Del(ctx, key)
				invalidated++
				slog.Debug("quote_delete_queued", "component", "cache", "coin", quote.Coin, "exchange", quote.Exchange, "key", key)
			} else {
				pipe.Do(ctx, "SET", key, data, "PXAT", quote.ExpiresAt.UnixMilli())
				saved++
				slog.Debug("quote_write_queued", "component", "cache", "coin", quote.Coin, "exchange", quote.Exchange, "key", key, "source_at", quote.SourceAt, "expires_at", quote.ExpiresAt, "remaining_ttl", ttl)
			}

			pipe.Publish(ctx, contracts.QuotesChannel, data)
		}

		return nil
	})

	slog.Debug("quote_batch_finished", "component", "cache", "batch_size", len(quotes), "writes_queued", saved, "deletions_queued", invalidated, "skipped", skipped, "error", err)

	return err
}
