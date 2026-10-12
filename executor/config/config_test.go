package config

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/fanoxiz/crypto-monitor/internal/testsupport"
)

func TestLoadDefaults(t *testing.T) {
	testsupport.ResetEnv(t)

	cfg, err := Load(testsupport.ConfigFile(t, "config.yaml", "{}\n"))
	if err != nil {
		t.Fatal(err)
	}

	want := Config{
		GRPCPort:    "8082",
		DatabaseURL: "postgres://crypto_user:secret_password@localhost:5433/crypto_db",
	}

	if cfg != want {
		t.Fatalf("configuration = %#v, want %#v", cfg, want)
	}
}

func TestLoadYAMLValuesWithoutEnvironment(t *testing.T) {
	testsupport.ResetEnv(t)

	contents := "grpc_port: '18082'\ndatabase_url: postgres://test:fixture@127.0.0.1:15433/history?sslmode=disable\n"
	cfg, err := Load(testsupport.ConfigFile(t, "config.yaml", contents))
	if err != nil {
		t.Fatal(err)
	}

	want := Config{
		GRPCPort:    "18082",
		DatabaseURL: "postgres://test:fixture@127.0.0.1:15433/history?sslmode=disable",
	}

	if cfg != want {
		t.Fatalf("YAML configuration = %#v, want %#v", cfg, want)
	}
}

func TestLoadEnvironmentOverridesYAML(t *testing.T) {
	testsupport.ResetEnv(t)
	t.Setenv("EXECUTOR_GRPC_PORT", "18082")
	t.Setenv("DATABASE_URL", "postgres://test:fixture@127.0.0.1:15433/history?sslmode=disable")

	cfg, err := Load(testsupport.ConfigFile(t, "config.yaml", "grpc_port: '9000'\ndatabase_url: postgres://yaml:fixture@yaml/db\n"))
	if err != nil {
		t.Fatal(err)
	}

	if cfg.GRPCPort != "18082" || cfg.DatabaseURL != "postgres://test:fixture@127.0.0.1:15433/history?sslmode=disable" {
		t.Fatalf("environment overrides not applied: %#v", cfg)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	for name, contents := range map[string]string{
		"broken_yaml":         "grpc_port: [",
		"wrong_port_type":     "grpc_port: {}\n",
		"wrong_database_type": "database_url: [one, two]\n",
	} {
		t.Run(name, func(t *testing.T) {
			testsupport.ResetEnv(t)

			cfg, err := Load(testsupport.ConfigFile(t, "config.yaml", contents))
			if err == nil {
				t.Fatalf("invalid configuration accepted: %#v", cfg)
			}

			if !reflect.DeepEqual(cfg, Config{}) {
				t.Fatalf("error exposed partially loaded configuration: %#v", cfg)
			}
		})
	}
}

func TestLoadRejectsMissingFile(t *testing.T) {
	testsupport.ResetEnv(t)

	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("missing configuration accepted")
	}
}
