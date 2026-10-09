package cache

import (
	"context"
	"encoding/json"
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
	_, err := s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, quote := range quotes {
			ttl := s.ttl - time.Since(quote.ReceivedAt)
			if ttl <= 0 || ttl > s.ttl {
				continue
			}

			data, err := json.Marshal(quote)
			if err != nil {
				return err
			}

			pipe.Set(ctx, contracts.QuoteKey(quote.Coin, quote.Exchange), data, ttl)
			pipe.Publish(ctx, contracts.QuotesChannel, data)
		}

		return nil
	})

	return err
}
