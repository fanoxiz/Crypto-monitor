package startup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/contracts/grpcpb"
	"github.com/fanoxiz/crypto-monitor/internal/testsupport"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var binaries = make(map[string]string)

func TestMain(m *testing.M) {
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	root := filepath.Clean(filepath.Join(cwd, "..", ".."))
	dir, err := os.MkdirTemp("", "cryptomonitor-block1-binaries-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	for _, service := range []string{"fetcher", "analyzer", "executor"} {
		binaries[service] = filepath.Join(dir, service)
		cmd := exec.Command("go", "build", "-race", "-buildvcs=false", "-o", binaries[service], "./"+service)
		cmd.Dir = root

		if output, err := cmd.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "build startup fixture %s: %v\n%s", service, err, output)
			os.RemoveAll(dir)
			os.Exit(1)
		}
	}

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type processLog struct {
	mutex sync.Mutex
	data  bytes.Buffer
}

func (l *processLog) Write(data []byte) (int, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.data.Write(data)
}

func (l *processLog) String() string {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return l.data.String()
}

type serviceProcess struct {
	cmd  *exec.Cmd
	logs processLog
	done chan struct{}
	err  error
}

func startService(t *testing.T, service, contents string, values map[string]string) *serviceProcess {
	t.Helper()

	root := t.TempDir()
	path := filepath.Join(root, service)

	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(path, "config.yaml"), []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}

	process := &serviceProcess{
		cmd:  exec.Command(binaries[service]),
		done: make(chan struct{}),
	}
	process.cmd.Dir = root
	process.cmd.Stdout = &process.logs
	process.cmd.Stderr = &process.logs

	blocked := make(map[string]bool)

	for _, key := range testsupport.ServiceEnv {
		blocked[key] = true
	}

	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		if !blocked[key] {
			process.cmd.Env = append(process.cmd.Env, value)
		}
	}

	process.cmd.Env = append(process.cmd.Env, "LOG_LEVEL=INFO")

	for key, value := range values {
		process.cmd.Env = append(process.cmd.Env, key+"="+value)
	}

	if err := process.cmd.Start(); err != nil {
		t.Fatal(err)
	}

	go func() {
		process.err = process.cmd.Wait()
		close(process.done)
	}()

	t.Cleanup(func() {
		process.stop(t)
	})

	return process
}

func (p *serviceProcess) waitLog(t *testing.T, message string) {
	t.Helper()

	testsupport.Await(t, func() bool {
		select {
		case <-p.done:
			t.Fatalf("service exited during startup: %v\n%s", p.err, p.logs.String())
		default:
		}

		return strings.Contains(p.logs.String(), message)
	}, "service did not reach startup stage "+message)
}

func (p *serviceProcess) stop(t *testing.T) {
	t.Helper()

	select {
	case <-p.done:
	default:
		if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
			t.Error(err)
		}

		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			p.cmd.Process.Kill()
			<-p.done
			t.Errorf("service failed to stop: %s", p.logs.String())
		}
	}

	if p.err != nil {
		t.Errorf("service exit: %v\n%s", p.err, p.logs.String())
	}
}

func testRedis(t *testing.T) *redis.Client {
	t.Helper()

	address := os.Getenv("TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("requires an isolated Redis instance in TEST_REDIS_ADDR")
	}

	client := redis.NewClient(&redis.Options{
		Addr: address,
	})
	t.Cleanup(func() {
		client.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis startup fixture: %v", err)
	}

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	return client
}

type subscription struct {
	exchange string
	request  map[string]any
}

func exchangeFixture(t *testing.T) (*httptest.Server, <-chan subscription) {
	t.Helper()

	requests := make(chan subscription, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		defer conn.Close()
		var request map[string]any

		if err := conn.ReadJSON(&request); err != nil {
			return
		}

		exchange := strings.TrimPrefix(r.URL.Path, "/")
		requests <- subscription{
			exchange: exchange,
			request:  request,
		}

		for _, coin := range []string{"ETH", "SOL"} {
			if err := conn.WriteJSON(exchangeQuote(exchange, coin)); err != nil {
				return
			}
		}

		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}

			if string(raw) == "ping" {
				if err := conn.WriteMessage(websocket.TextMessage, []byte("pong")); err != nil {
					return
				}
			}

			if exchange == "Bybit" {
				var packet struct {
					Operation string `json:"op"`
					ID        string `json:"req_id"`
				}

				if json.Unmarshal(raw, &packet) == nil && packet.Operation == "ping" {
					if err := conn.WriteJSON(map[string]any{
						"op":      "ping",
						"req_id":  packet.ID,
						"success": true,
						"ret_msg": "pong",
					}); err != nil {
						return
					}
				}
			}
		}
	}))

	t.Cleanup(server.Close)

	return server, requests
}

func exchangeQuote(exchange, coin string) any {
	at := time.Now().UnixMilli()
	bids := [][]string{{"100", "2"}}
	asks := [][]string{{"101", "3"}}

	switch exchange {
	case "Binance":
		return map[string]any{
			"s": coin + "USDT",
			"b": "100",
			"B": "2",
			"a": "101",
			"A": "3",
		}
	case "Bybit":
		return map[string]any{
			"cts": at,
			"data": map[string]any{
				"s": coin + "USDT",
				"b": bids,
				"a": asks,
			},
		}
	case "Bitget":
		return map[string]any{
			"arg": map[string]string{
				"symbol": coin + "USDT",
			},
			"data": []any{map[string]any{
				"b":  bids,
				"a":  asks,
				"ts": fmt.Sprint(at),
			}},
		}
	default:
		return map[string]any{
			"arg": map[string]string{
				"instId": coin + "-USDT",
			},
			"data": []any{map[string]any{
				"bids": bids,
				"asks": asks,
				"ts":   fmt.Sprint(at),
			}},
		}
	}
}

func TestFetcherUsesConfiguredSourcesCoinsRedisAndTTL(t *testing.T) {
	client := testRedis(t)
	server, requests := exchangeFixture(t)
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	values := map[string]string{
		"REDIS_ADDR":        client.Options().Addr,
		"TRACKED_COINS":     " eth , sol ",
		"FETCHER_PRICE_TTL": "2s",
		"BINANCE_WS_URL":    endpoint + "/Binance",
		"BYBIT_WS_URL":      endpoint + "/Bybit",
		"BITGET_WS_URL":     endpoint + "/Bitget",
		"OKX_WS_URL":        endpoint + "/OKX",
	}
	contents := `tracked_coins: [BTC]
redis_addr: 127.0.0.1:1
price_ttl: 9s
binance_ws_url: ws://127.0.0.1:1/binance
bybit_ws_url: ws://127.0.0.1:1/bybit
bitget_ws_url: ws://127.0.0.1:1/bitget
okx_ws_url: ws://127.0.0.1:1/okx
`
	process := startService(t, "fetcher", contents, values)
	seen := make(map[string]bool)

	for range 4 {
		select {
		case received := <-requests:
			if seen[received.exchange] {
				t.Fatalf("source connected twice: %s", received.exchange)
			}

			seen[received.exchange] = true
			want := expectedSubscription(received.exchange)

			if !reflect.DeepEqual(received.request, want) {
				t.Fatalf("%s subscription = %#v, want %#v", received.exchange, received.request, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("configured sources did not connect: %s", process.logs.String())
		}
	}

	for _, exchange := range []string{"Binance", "Bybit", "Bitget", "OKX"} {
		for _, coin := range []string{"ETH", "SOL"} {
			key := contracts.QuoteKey(coin, exchange)
			var quote contracts.Quote

			testsupport.Await(t, func() bool {
				raw, err := client.Get(context.Background(), key).Bytes()
				return err == nil && json.Unmarshal(raw, &quote) == nil
			}, "configured Redis did not receive "+key)

			if quote.Coin != coin || quote.Exchange != exchange {
				t.Fatalf("wrong normalized quote at %s: %#v", key, quote)
			}

			base := quote.SourceAt
			lifetime := 2 * time.Second

			if exchange == "Binance" {
				base = quote.ReceivedAt
				lifetime /= 2
			}

			if quote.ExpiresAt.UnixMilli() != base.Add(lifetime).UnixMilli() {
				t.Fatalf("%s did not use configured price_ttl: %#v", key, quote)
			}
		}
	}

	if !strings.Contains(process.logs.String(), "price_ttl=2s") {
		t.Fatal("effective price TTL was not reported")
	}
}

func expectedSubscription(exchange string) map[string]any {
	switch exchange {
	case "Binance":
		return map[string]any{
			"method": "SUBSCRIBE",
			"params": []any{"ethusdt@bookTicker", "solusdt@bookTicker"},
			"id":     float64(1),
		}
	case "Bybit":
		return map[string]any{
			"op":     "subscribe",
			"args":   []any{"orderbook.1.ETHUSDT", "orderbook.1.SOLUSDT"},
			"req_id": "quotes",
		}
	default:
		args := make([]any, 0, 2)

		for _, coin := range []string{"ETH", "SOL"} {
			if exchange == "Bitget" {
				args = append(args, map[string]any{
					"instType": "spot",
					"topic":    "books1",
					"symbol":   coin + "USDT",
				})
			} else {
				args = append(args, map[string]any{
					"channel": "bbo-tbt",
					"instId":  coin + "-USDT",
				})
			}
		}

		return map[string]any{
			"op":   "subscribe",
			"args": args,
		}
	}
}

type dealReceiver struct {
	grpcpb.UnimplementedExecutorServiceServer
	deals chan *grpcpb.ProfitDealInfo
}

func (r *dealReceiver) ProcessDeal(ctx context.Context, deal *grpcpb.ProfitDealInfo) (*emptypb.Empty, error) {
	select {
	case r.deals <- deal:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return &emptypb.Empty{}, nil
}

func TestAnalyzerUsesConfiguredCoinsFeesAgeRedisAndGRPCTarget(t *testing.T) {
	client := testRedis(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	receiver := &dealReceiver{
		deals: make(chan *grpcpb.ProfitDealInfo, 4),
	}
	server := grpc.NewServer()
	grpcpb.RegisterExecutorServiceServer(server, receiver)

	go server.Serve(listener)
	t.Cleanup(server.Stop)

	contents := `tracked_coins: [BTC]
exchanges: [Binance, Bybit]
redis_addr: 127.0.0.1:1
executor_grpc_addr: 127.0.0.1:1
max_quote_age: 100ms
fees:
  Binance: 0.001
  Bybit: 0.002
  Coinbase: 0.009
`
	process := startService(t, "analyzer", contents, map[string]string{
		"TRACKED_COINS":          " eth ",
		"REDIS_ADDR":             client.Options().Addr,
		"EXECUTOR_GRPC_ADDR":     listener.Addr().String(),
		"ANALYZER_MAX_QUOTE_AGE": "2s",
	})
	process.waitLog(t, "quotes_subscribed")
	at := time.Now().UTC().Add(-800 * time.Millisecond)

	for _, input := range []struct {
		exchange string
		bid      string
		ask      string
	}{
		{
			exchange: "Coinbase",
			bid:      "200",
			ask:      "201",
		},
		{
			exchange: "Binance",
			bid:      "100",
			ask:      "101",
		},
		{
			exchange: "Bybit",
			bid:      "103",
			ask:      "104",
		},
	} {
		quote := contracts.Quote{
			Coin:       "ETH",
			Exchange:   input.exchange,
			BidPrice:   input.bid,
			BidSize:    "3",
			AskPrice:   input.ask,
			AskSize:    "8",
			SourceAt:   at,
			ReceivedAt: at,
			ExpiresAt:  at.Add(2 * time.Second),
		}
		payload, err := json.Marshal(quote)
		if err != nil {
			t.Fatal(err)
		}

		if err := client.Publish(context.Background(), contracts.QuotesChannel, payload).Err(); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case deal := <-receiver.deals:
		if deal.GetCoin() != "ETH" || deal.GetBuyExchange() != "Binance" || deal.GetSellExchange() != "Bybit" {
			t.Fatalf("configured analyzer produced unexpected deal: %v", deal)
		}

		for _, amount := range []struct {
			name string
			got  float64
			want float64
		}{
			{
				name: "buy amount",
				got:  deal.GetBuyAmountUsdt(),
				want: 303.303,
			},
			{
				name: "sell amount",
				got:  deal.GetSellAmountUsdt(),
				want: 308.382,
			},
			{
				name: "profit",
				got:  deal.GetProfitUsdt(),
				want: 5.079,
			},
		} {
			if math.IsNaN(amount.got) || math.Abs(amount.got-amount.want) > 1e-9 {
				t.Errorf("configured %s = %.12f, want %.12f", amount.name, amount.got, amount.want)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("configured gRPC target received no signal: %s", process.logs.String())
	}
}

func TestExecutorUsesConfiguredPortAndDatabase(t *testing.T) {
	admin, dsn, schema := testsupport.PostgresDatabase(t, false)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	address := listener.Addr().String()
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}

	listener.Close()
	process := startService(t, "executor", "grpc_port: invalid-port\ndatabase_url: invalid-database\n", map[string]string{
		"EXECUTOR_GRPC_PORT": port,
		"DATABASE_URL":       dsn,
	})
	process.waitLog(t, "server_started")
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		conn.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	at := time.Now().UTC()

	_, err = grpcpb.NewExecutorServiceClient(conn).ProcessDeal(ctx, &grpcpb.ProfitDealInfo{
		Coin:           "SOL",
		BuyExchange:    "Binance",
		SellExchange:   "Bybit",
		BuyAmountUsdt:  100,
		SellAmountUsdt: 102,
		ProfitUsdt:     2,
		DetectedAt:     timestamppb.New(at),
	})
	if err != nil {
		t.Fatal(err)
	}

	var payload []byte

	if err := admin.QueryRow(ctx, "SELECT payload FROM "+schema+".profit_deals").Scan(&payload); err != nil {
		t.Fatal(err)
	}

	var saved contracts.ProfitDealInfo

	if err := json.Unmarshal(payload, &saved); err != nil {
		t.Fatal(err)
	}

	if saved.Coin != "SOL" || saved.BuyExchange != "Binance" || saved.SellExchange != "Bybit" || saved.ProfitUSDT != 2 || !saved.DetectedAt.Equal(at) {
		t.Fatalf("configured database received unexpected payload: %#v", saved)
	}
}
