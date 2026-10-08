package config

import "github.com/ilyakaznacheev/cleanenv"

type Config struct {
	GRPCPort    string `yaml:"grpc_port" env:"EXECUTOR_GRPC_PORT" env-default:"8082"`
	DatabaseURL string `yaml:"database_url" env:"DATABASE_URL" env-default:"postgres://crypto_user:secret_password@localhost:5433/crypto_db"`
}

func Load(path string) (Config, error) {
	var cfg Config

	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}
