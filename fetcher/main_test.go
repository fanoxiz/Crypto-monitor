package main

import (
	"strings"
	"testing"

	"github.com/fanoxiz/crypto-monitor/internal/testsupport"
)

const startupConfig = `tracked_coins: [BTC]
redis_addr: localhost:6379
binance_ws_url: ws://127.0.0.1:1/binance
bybit_ws_url: ws://127.0.0.1:1/bybit
bitget_ws_url: ws://127.0.0.1:1/bitget
okx_ws_url: ws://127.0.0.1:1/okx
`

func TestRunConfigurationFailureStartsNoDependency(t *testing.T) {
	for name, contents := range map[string]string{
		"missing":          "",
		"broken":           "tracked_coins: [",
		"incompatible":     "tracked_coins: {}\n",
		"invalid_required": "tracked_coins: []\n",
	} {
		t.Run(name, func(t *testing.T) {
			testsupport.ResetEnv(t)
			probe := testsupport.NewDependencyProbe(t, "")
			t.Setenv("REDIS_ADDR", probe.Address)
			testsupport.ServiceWorkdir(t, "fetcher", contents)
			if name == "missing" {
				t.Chdir(t.TempDir())
			}

			if err := run(); err == nil {
				t.Fatal("startup accepted invalid configuration")
			}

			if probe.Accepted() != 0 {
				t.Fatal("dependency contacted before configuration was validated")
			}
		})
	}
}

func TestRunRejectsUnavailableRedisAndClosesClient(t *testing.T) {
	testsupport.ResetEnv(t)
	probe := testsupport.NewDependencyProbe(t, "-ERR startup dependency unavailable\r\n")
	t.Setenv("REDIS_ADDR", probe.Address)
	testsupport.ServiceWorkdir(t, "fetcher", startupConfig)

	err := run()
	if err == nil || !strings.Contains(err.Error(), "startup dependency unavailable") {
		t.Fatalf("startup error = %v, want rejected Redis ping", err)
	}

	if probe.Accepted() == 0 {
		t.Fatal("configured Redis endpoint was not contacted")
	}

	testsupport.Await(t, func() bool {
		return probe.Open() == 0
	}, "Redis connection remained open after startup failed")
}
