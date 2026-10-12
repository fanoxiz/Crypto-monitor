package main

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/fanoxiz/crypto-monitor/internal/testsupport"
)

func TestRunConfigurationFailureStartsNoDependency(t *testing.T) {
	for name, contents := range map[string]string{
		"missing":      "",
		"broken":       "grpc_port: [",
		"incompatible": "grpc_port: {}\n",
	} {
		t.Run(name, func(t *testing.T) {
			testsupport.ResetEnv(t)
			probe := testsupport.NewDependencyProbe(t, "")
			t.Setenv("DATABASE_URL", "postgres://fixture:fixture@"+probe.Address+"/fixture?sslmode=disable")
			testsupport.ServiceWorkdir(t, "executor", contents)

			if name == "missing" {
				t.Chdir(t.TempDir())
			}

			if err := run(); err == nil {
				t.Fatal("startup accepted invalid configuration")
			}

			if probe.Accepted() != 0 {
				t.Fatal("database contacted before configuration was parsed")
			}
		})
	}
}

func TestRunRejectsUnavailableDatabase(t *testing.T) {
	testsupport.ResetEnv(t)
	probe := testsupport.NewDependencyProbe(t, "")
	t.Setenv("DATABASE_URL", "postgres://fixture:fixture@"+probe.Address+"/fixture?sslmode=disable&connect_timeout=1")
	testsupport.ServiceWorkdir(t, "executor", "grpc_port: '0'\n")

	if err := run(); err == nil || !strings.Contains(err.Error(), "postgres init schema") {
		t.Fatalf("startup error = %v, want unavailable database", err)
	}

	if probe.Accepted() == 0 {
		t.Fatal("configured database endpoint was not contacted")
	}
}

func TestRunSchemaPreparationFailureReleasesPool(t *testing.T) {
	testsupport.ResetEnv(t)
	admin, dsn, application := testsupport.PostgresDatabase(t, true)
	t.Setenv("DATABASE_URL", dsn)
	testsupport.ServiceWorkdir(t, "executor", "grpc_port: '0'\n")

	if err := run(); err == nil || !strings.Contains(err.Error(), "postgres init schema") || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("startup error = %v, want schema permission failure", err)
	}

	testsupport.AssertPostgresReleased(t, admin, application)
}

func TestRunOccupiedPortReleasesPool(t *testing.T) {
	testsupport.ResetEnv(t)
	admin, dsn, application := testsupport.PostgresDatabase(t, false)
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}

	defer listener.Close()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("DATABASE_URL", dsn)
	t.Setenv("EXECUTOR_GRPC_PORT", port)
	testsupport.ServiceWorkdir(t, "executor", "grpc_port: '0'\n")

	if err := run(); err == nil || !strings.Contains(err.Error(), "address already in use") {
		t.Fatalf("startup error = %v, want occupied port", err)
	}

	var table bool

	if err := admin.QueryRow(context.Background(), "SELECT to_regclass($1) IS NOT NULL", application+".profit_deals").Scan(&table); err != nil {
		t.Fatal(err)
	}

	if !table {
		t.Fatal("database schema was not prepared before the listener failed")
	}

	testsupport.AssertPostgresReleased(t, admin, application)
}
