package testsupport

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type DependencyProbe struct {
	Address  string
	accepted atomic.Int32
	pings    atomic.Int32
	mutex    sync.Mutex
	conns    map[net.Conn]bool
}

func NewDependencyProbe(t *testing.T, redisPingReply string) *DependencyProbe {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	probe := &DependencyProbe{
		Address: listener.Addr().String(),
		conns:   make(map[net.Conn]bool),
	}
	acceptedDone := make(chan struct{})
	var workers sync.WaitGroup

	go func() {
		defer close(acceptedDone)

		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			probe.accepted.Add(1)
			probe.mutex.Lock()
			probe.conns[conn] = true
			probe.mutex.Unlock()

			workers.Go(func() {
				defer func() {
					conn.Close()
					probe.mutex.Lock()
					delete(probe.conns, conn)
					probe.mutex.Unlock()
				}()

				if redisPingReply == "" {
					return
				}

				reader := bufio.NewReader(conn)

				for {
					args, err := readCommand(reader)
					if err != nil || len(args) == 0 {
						return
					}

					response := "+OK\r\n"

					switch strings.ToUpper(args[0]) {
					case "HELLO":
						response = "-ERR unknown command 'HELLO'\r\n"
					case "PING":
						probe.pings.Add(1)
						response = redisPingReply
					}

					if _, err := io.WriteString(conn, response); err != nil {
						return
					}
				}
			})
		}
	}()

	t.Cleanup(func() {
		listener.Close()
		<-acceptedDone

		probe.mutex.Lock()

		for conn := range probe.conns {
			conn.Close()
		}

		probe.mutex.Unlock()
		workers.Wait()
	})

	return probe
}

func readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected RESP array, received %q", line)
	}

	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}

	args := make([]string, count)

	for i := range args {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		length, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "$")))
		if err != nil || length < 0 {
			return nil, fmt.Errorf("invalid RESP bulk length %q", line)
		}

		data := make([]byte, length+2)

		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}

		args[i] = string(data[:length])
	}

	return args, nil
}

func (p *DependencyProbe) Accepted() int {
	return int(p.accepted.Load())
}

func (p *DependencyProbe) Pings() int {
	return int(p.pings.Load())
}

func (p *DependencyProbe) Open() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	return len(p.conns)
}

func Await(t *testing.T, condition func() bool, description string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal(description)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

var databaseSequence atomic.Int32

func PostgresDatabase(t *testing.T, denyCreate bool) (*pgx.Conn, string, string) {
	t.Helper()

	base := os.Getenv("TEST_POSTGRES_DSN")
	if base == "" {
		t.Skip("requires an isolated PostgreSQL instance in TEST_POSTGRES_DSN")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("PostgreSQL test fixture: %v", err)
	}

	t.Cleanup(func() {
		admin.Close(context.Background())
	})

	name := fmt.Sprintf("block1_%d_%d", os.Getpid(), databaseSequence.Add(1))
	identifier := pgx.Identifier{name}.Sanitize()

	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()

		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Error(err)
		}

		if denyCreate {
			if _, err := admin.Exec(cleanup, "DROP ROLE "+identifier); err != nil {
				t.Error(err)
			}
		}
	})

	parsed, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}

	values := parsed.Query()
	values.Set("search_path", name)
	values.Set("application_name", name)
	parsed.RawQuery = values.Encode()

	if denyCreate {
		if _, err := admin.Exec(ctx, "CREATE ROLE "+identifier+" LOGIN PASSWORD 'block1_fixture'"); err != nil {
			t.Fatal(err)
		}

		if _, err := admin.Exec(ctx, "GRANT USAGE ON SCHEMA "+identifier+" TO "+identifier); err != nil {
			t.Fatal(err)
		}

		parsed.User = url.UserPassword(name, "block1_fixture")
	}

	return admin, parsed.String(), name
}

func AssertPostgresReleased(t *testing.T, admin *pgx.Conn, application string) {
	t.Helper()

	Await(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var count int

		if err := admin.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", application).Scan(&count); err != nil {
			t.Fatal(err)
		}

		return count == 0
	}, "PostgreSQL connections remained open after startup failed")
}
