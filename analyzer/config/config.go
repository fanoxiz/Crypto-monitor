package config

import (
	"fmt"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Port              string             `yaml:"port" env:"ANALYZER_PORT" env-default:"8081"`
	Fees              map[string]float64 `yaml:"fees"`
	ExecutorEndpoint  string             `yaml:"executor_url" env:"EXECUTOR_URL" env-default:"http://localhost:8082/deals"`
	HTTPClientTimeout time.Duration      `yaml:"http_client_timeout" env:"ANALYZER_HTTP_CLIENT_TIMEOUT" env-default:"2s"`
	RedisAddr         string             `yaml:"redis_addr" env:"REDIS_ADDR" env-default:"localhost:6379"`
	RedisPassword     string             `yaml:"redis_password" env:"REDIS_PASSWORD" env-default:""`
	RedisDB           int                `yaml:"redis_db" env:"REDIS_DB" env-default:"0"`
	RedisKeyPrefix    string             `yaml:"redis_key_prefix" env:"REDIS_KEY_PREFIX" env-default:"analyzer:prices:"`
	PriceTTL          time.Duration      `yaml:"price_ttl" env:"ANALYZER_PRICE_TTL" env-default:"10s"`
}

func Load(path string) (Config, error) {
	var cfg Config

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return Config{}, fmt.Errorf("config load: %w", err)
	}

	return cfg, nil
}
