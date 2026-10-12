package main

import (
	"strings"
	"testing"

	"github.com/fanoxiz/crypto-monitor/internal/testsupport"
)

const startupConfig = `tracked_coins: [BTC]
redis_addr: localhost:6379
exchanges: [Binance, Bybit]
fees:
  Binance: 0
  Bybit: 0
`

func TestRunConfigurationFailureStartsNoDependency(t *testing.T) {
	for name, contents := range map[string]string{
		"missing":          "",
		"broken":           "tracked_coins: [",
		"incompatible":     "fees: []\n",
		"invalid_required": "tracked_coins: []\n",
	} {
		t.Run(name, func(t *testing.T) {
			testsupport.ResetEnv(t)
			probe := testsupport.NewDependencyProbe(t, "")
			t.Setenv("REDIS_ADDR", probe.Address)
			testsupport.ServiceWorkdir(t, "analyzer", contents)
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
	testsupport.ServiceWorkdir(t, "analyzer", startupConfig)

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

func TestRunGRPCClientFailureAfterSuccessfulPingClosesRedis(t *testing.T) {
	testsupport.ResetEnv(t)
	probe := testsupport.NewDependencyProbe(t, "+PONG\r\n")
	t.Setenv("REDIS_ADDR", probe.Address)
	t.Setenv("EXECUTOR_GRPC_ADDR", "%")
	testsupport.ServiceWorkdir(t, "analyzer", startupConfig)

	err := run()
	if err == nil || !strings.Contains(err.Error(), "invalid URL escape") {
		t.Fatalf("startup error = %v, want invalid gRPC target after Redis ping", err)
	}

	if probe.Pings() != 1 {
		t.Fatalf("Redis ping count = %d, want one successful ping", probe.Pings())
	}

	testsupport.Await(t, func() bool {
		return probe.Open() == 0
	}, "Redis connection remained open after gRPC client creation failed")
}
