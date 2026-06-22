package config

import (
	"fmt"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	GRPCPort         string             `yaml:"grpc_port" env:"ANALYZER_GRPC_PORT" env-default:"8081"`
	Fees             map[string]float64 `yaml:"fees"`
	ExecutorGRPCAddr string             `yaml:"executor_grpc_addr" env:"EXECUTOR_GRPC_ADDR" env-default:"localhost:8082"`
	RedisAddr        string             `yaml:"redis_addr" env:"REDIS_ADDR" env-default:"localhost:6379"`
	RedisPassword    string             `yaml:"redis_password" env:"REDIS_PASSWORD" env-default:""`
	RedisDB          int                `yaml:"redis_db" env:"REDIS_DB" env-default:"0"`
	RedisKeyPrefix   string             `yaml:"redis_key_prefix" env:"REDIS_KEY_PREFIX" env-default:"analyzer:prices:"`
	PriceTTL         time.Duration      `yaml:"price_ttl" env:"ANALYZER_PRICE_TTL" env-default:"10s"`
}

func Load(path string) (Config, error) {
	var cfg Config

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return Config{}, fmt.Errorf("config load: %w", err)
	}

	return cfg, nil
}
