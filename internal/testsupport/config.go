package testsupport

import (
	"os"
	"path/filepath"
	"testing"
)

var ServiceEnv = []string{
	"TRACKED_COINS",
	"REDIS_ADDR",
	"FETCHER_PRICE_TTL",
	"BINANCE_WS_URL",
	"BYBIT_WS_URL",
	"BITGET_WS_URL",
	"OKX_WS_URL",
	"ANALYZER_MAX_QUOTE_AGE",
	"EXECUTOR_GRPC_ADDR",
	"EXECUTOR_GRPC_PORT",
	"DATABASE_URL",
	"LOG_LEVEL",
}

func ResetEnv(t *testing.T) {
	t.Helper()

	for _, key := range ServiceEnv {
		value, exists := os.LookupEnv(key)

		t.Cleanup(func() {
			if exists {
				os.Setenv(key, value)
			} else {
				os.Unsetenv(key)
			}
		})

		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}

func ConfigFile(t *testing.T, name, contents string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), name)

	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}

	return path
}

func ServiceWorkdir(t *testing.T, service, contents string) {
	t.Helper()

	root := t.TempDir()
	path := filepath.Join(root, service)

	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(path, "config.yaml"), []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}

	t.Chdir(root)
}
