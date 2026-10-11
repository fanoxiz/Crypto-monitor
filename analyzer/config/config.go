package config

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	TrackedCoins     []string           `yaml:"tracked_coins" env:"TRACKED_COINS"`
	Exchanges        []string           `yaml:"exchanges"`
	RedisAddr        string             `yaml:"redis_addr" env:"REDIS_ADDR" env-default:"localhost:6379"`
	MaxQuoteAge      time.Duration      `yaml:"max_quote_age" env:"ANALYZER_MAX_QUOTE_AGE" env-default:"600ms"`
	Fees             map[string]float64 `yaml:"fees"`
	ExecutorGRPCAddr string             `yaml:"executor_grpc_addr" env:"EXECUTOR_GRPC_ADDR" env-default:"localhost:8082"`
}

func Load(path string) (Config, error) {
	var cfg Config

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return Config{}, err
	}

	coins := make(map[string]bool)

	for i, coin := range cfg.TrackedCoins {
		coin = strings.ToUpper(strings.TrimSpace(coin))
		if coin == "" || coins[coin] {
			return Config{}, fmt.Errorf("empty or duplicate coin: %q", coin)
		}

		cfg.TrackedCoins[i] = coin
		coins[coin] = true
	}

	if len(cfg.TrackedCoins) == 0 || len(cfg.Exchanges) < 2 || cfg.MaxQuoteAge <= 0 {
		return Config{}, fmt.Errorf("configure coins, at least two exchanges and positive max_quote_age")
	}

	exchanges := make(map[string]bool)

	for _, exchange := range cfg.Exchanges {
		if exchange == "" || exchanges[exchange] {
			return Config{}, fmt.Errorf("empty or duplicate exchange: %q", exchange)
		}

		fee, exists := cfg.Fees[exchange]
		if !exists || math.IsNaN(fee) || math.IsInf(fee, 0) || fee < 0 || fee >= 1 {
			return Config{}, fmt.Errorf("invalid or missing fee for %s", exchange)
		}

		exchanges[exchange] = true
	}

	return cfg, nil
}
