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

func (s *RedisQuoteStore) Save(ctx context.Context, quote contracts.Quote) error {
	data, err := json.Marshal(quote)
	if err != nil {
		return err
	}

	key := "fetcher:quotes:spot:" + quote.Coin + "-USDT:" + quote.Exchange

	return s.client.Set(ctx, key, data, s.ttl).Err()
}
