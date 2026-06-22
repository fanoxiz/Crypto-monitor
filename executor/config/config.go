package config

import (
	"fmt"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	GRPCPort       string  `yaml:"grpc_port" env:"EXECUTOR_GRPC_PORT" env-default:"8082"`
	TradeSize      float64 `yaml:"trade_size" env:"EXECUTOR_TRADE_SIZE" env-default:"10000"`
	InitialBalance float64 `yaml:"initial_balance" env:"EXECUTOR_INITIAL_BALANCE" env-default:"10000"`
}

func Load(path string) (Config, error) {
	var cfg Config

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return Config{}, fmt.Errorf("config load: %w", err)
	}

	return cfg, nil
}
