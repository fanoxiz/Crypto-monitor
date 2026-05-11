package cache

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/redis/go-redis/v9"
)

type RedisPriceStore struct {
	client    *redis.Client
	keyPrefix string
}

func NewRedisPriceStore(client *redis.Client, keyPrefix string) *RedisPriceStore {
	return &RedisPriceStore{
		client:    client,
		keyPrefix: keyPrefix,
	}
}

func (s *RedisPriceStore) SetPrice(coin, exchange string, price contracts.BidAsk) error {
	data, err := json.Marshal(price)
	if err != nil {
		return fmt.Errorf("redis set price: marshal: %w", err)
	}

	if err := s.client.HSet(context.Background(), s.coinKey(coin), exchange, data).Err(); err != nil {
		return fmt.Errorf("redis set price: hset: %w", err)
	}

	return nil
}

func (s *RedisPriceStore) GetPrices(coin string) (map[string]contracts.BidAsk, error) {
	rows, err := s.client.HGetAll(context.Background(), s.coinKey(coin)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis get prices: hgetall: %w", err)
	}

	prices := make(map[string]contracts.BidAsk, len(rows))
	for exchange, raw := range rows {
		var price contracts.BidAsk
		if err := json.Unmarshal([]byte(raw), &price); err != nil {
			continue
		}

		prices[exchange] = price
	}

	return prices, nil
}

func (s *RedisPriceStore) coinKey(coin string) string {
	if s.keyPrefix == "" {
		return "analyzer:prices:" + coin
	}

	return s.keyPrefix + coin
}
