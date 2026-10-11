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
			ttl := s.ttl - time.Since(quote.ReceivedAt)
			if ttl <= 0 || ttl > s.ttl {
				skipped++
				slog.Debug("quote_write_skipped", "component", "cache", "coin", quote.Coin, "exchange", quote.Exchange, "reason", "invalid_age", "age", time.Since(quote.ReceivedAt), "remaining_ttl", ttl, "invalidated", quote.Invalidated)

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
				pipe.Set(ctx, key, data, ttl)
				saved++
				slog.Debug("quote_write_queued", "component", "cache", "coin", quote.Coin, "exchange", quote.Exchange, "key", key, "remaining_ttl", ttl)
			}

			pipe.Publish(ctx, contracts.QuotesChannel, data)
		}

		return nil
	})

	slog.Debug("quote_batch_finished", "component", "cache", "batch_size", len(quotes), "writes_queued", saved, "deletions_queued", invalidated, "skipped", skipped, "error", err)

	return err
}
