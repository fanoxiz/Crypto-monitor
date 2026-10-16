package cache

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/redis/go-redis/v9"
)

const saveQuoteScript = `
if ARGV[1] == "DEL" then
	redis.call("DEL", KEYS[1])
else
	redis.call("SET", KEYS[1], ARGV[2], "PXAT", ARGV[3])
end
return redis.call("PUBLISH", ARGV[4], ARGV[2])
`

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
			operation := "SET"

			if quote.Invalidated {
				operation = "DEL"
				invalidated++
				slog.Debug("quote_delete_queued", "component", "cache", "coin", quote.Coin, "exchange", quote.Exchange, "key", key)
			} else {
				saved++
				slog.Debug("quote_write_queued", "component", "cache", "coin", quote.Coin, "exchange", quote.Exchange, "key", key, "source_at", quote.SourceAt, "expires_at", quote.ExpiresAt, "remaining_ttl", ttl)
			}

			pipe.Eval(ctx, saveQuoteScript, []string{key}, operation, data, quote.ExpiresAt.UnixMilli(), contracts.QuotesChannel)
		}

		return nil
	})

	slog.Debug("quote_batch_finished", "component", "cache", "batch_size", len(quotes), "writes_queued", saved, "deletions_queued", invalidated, "skipped", skipped, "error", err)

	return err
}
