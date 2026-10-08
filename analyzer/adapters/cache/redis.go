package cache

import (
	"context"
	"encoding/json"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/redis/go-redis/v9"
)

type RedisQuoteStore struct {
	client *redis.Client
	keys   []string
}

func NewRedisQuoteStore(client *redis.Client, coins, exchanges []string) *RedisQuoteStore {
	keys := make([]string, 0, len(coins)*len(exchanges))

	for _, coin := range coins {
		for _, exchange := range exchanges {
			keys = append(keys, quoteKey(coin, exchange))
		}
	}

	return &RedisQuoteStore{
		client: client,
		keys:   keys,
	}
}

func (s *RedisQuoteStore) GetQuotes(ctx context.Context) ([]contracts.Quote, error) {
	values, err := s.client.MGet(ctx, s.keys...).Result()
	if err != nil {
		return nil, err
	}

	quotes := make([]contracts.Quote, 0, len(values))

	for i, value := range values {
		raw, exists := value.(string)
		if !exists {
			continue
		}

		var quote contracts.Quote

		if err := json.Unmarshal([]byte(raw), &quote); err != nil {
			continue
		}

		if quoteKey(quote.Coin, quote.Exchange) != s.keys[i] {
			continue
		}

		quotes = append(quotes, quote)
	}

	return quotes, nil
}

func quoteKey(coin, exchange string) string {
	return "fetcher:quotes:spot:" + coin + "-USDT:" + exchange
}
