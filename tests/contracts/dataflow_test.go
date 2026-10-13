package dataflow_test

import (
	"context"
	"encoding/json"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/analyzer/adapters/sender"
	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/contracts/grpcpb"
	"github.com/fanoxiz/crypto-monitor/executor/adapters/db"
	"github.com/fanoxiz/crypto-monitor/executor/adapters/receiver"
	"github.com/fanoxiz/crypto-monitor/executor/core"
	"github.com/fanoxiz/crypto-monitor/fetcher/adapters/cache"
	"github.com/fanoxiz/crypto-monitor/internal/testsupport"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestQuoteRedisContractPreservesFieldsAndInvalidation(t *testing.T) {
	client := testsupport.RedisClient(t, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pubsub := client.Subscribe(ctx, contracts.QuotesChannel)
	defer pubsub.Close()
	stop := context.AfterFunc(ctx, func() {
		pubsub.Close()
	})
	defer stop()

	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatal(err)
	}

	receivedAt := time.Now().UTC().Add(-time.Second).Truncate(time.Second).Add(123456789 * time.Nanosecond)
	sourceAt := receivedAt.Truncate(time.Millisecond).Add(-100 * time.Millisecond)
	store := cache.NewRedisQuoteStore(client, 10*time.Second)
	quotes := make([]contracts.Quote, 0, 4)

	for _, exchange := range []string{"Binance", "Bybit", "Bitget", "OKX"} {
		quote := contracts.Quote{
			Coin:       "XAUT",
			Exchange:   exchange,
			BidPrice:   "99.1234567800",
			BidSize:    "1.2345678900",
			AskPrice:   "100.9876543200",
			AskSize:    "2.3456789100",
			SourceAt:   sourceAt,
			ReceivedAt: receivedAt,
		}
		if exchange == "Binance" {
			quote.SourceAt = time.Time{}
		}

		quotes = append(quotes, quote)
	}

	if err := store.SaveBatch(ctx, quotes); err != nil {
		t.Fatal(err)
	}

	for _, input := range quotes {
		want := input
		want.ExpiresAt = sourceAt.Add(10 * time.Second)
		if input.Exchange == "Binance" {
			want.ExpiresAt = receivedAt.Add(5 * time.Second).Truncate(time.Millisecond)
		}

		payload, event := receiveContractQuote(t, ctx, pubsub)
		if !reflect.DeepEqual(event, want) {
			t.Fatalf("published quote fields changed: %#v, want %#v", event, want)
		}

		key := "fetcher:quotes:spot:XAUT-USDT:" + input.Exchange
		stored, err := client.Get(ctx, key).Result()
		if err != nil {
			t.Fatalf("expected quote key %s: %v", key, err)
		}

		if stored != payload {
			t.Fatalf("Redis value differs from published quote for %s", key)
		}

		var fields map[string]json.RawMessage

		if err := json.Unmarshal([]byte(stored), &fields); err != nil {
			t.Fatal(err)
		}

		for _, field := range []string{"coin", "exchange", "bid_price", "bid_size", "ask_price", "ask_size", "source_at", "received_at", "expires_at"} {
			if _, exists := fields[field]; !exists {
				t.Errorf("quote JSON lost contract field %s", field)
			}
		}
	}

	invalidated := quotes[1]
	invalidated.Invalidated = true

	if err := store.SaveBatch(ctx, []contracts.Quote{invalidated}); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Get(ctx, "fetcher:quotes:spot:XAUT-USDT:Bybit").Result(); err != redis.Nil {
		t.Fatalf("invalidation left a stored price: %v", err)
	}

	invalidated.ExpiresAt = receivedAt.Add(10 * time.Second).Truncate(time.Millisecond)
	_, event := receiveContractQuote(t, ctx, pubsub)
	if !reflect.DeepEqual(event, invalidated) {
		t.Fatalf("invalidation event fields changed: %#v, want %#v", event, invalidated)
	}
}

func receiveContractQuote(t *testing.T, ctx context.Context, pubsub *redis.PubSub) (string, contracts.Quote) {
	t.Helper()

	for {
		message, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}

		var quote contracts.Quote

		if err := json.Unmarshal([]byte(message.Payload), &quote); err != nil {
			t.Fatal(err)
		}

		if quote.Coin == "XAUT" {
			return message.Payload, quote
		}
	}
}

func TestSignalGRPCAndJSONBContractPreservesFields(t *testing.T) {
	admin, dsn, schema := testsupport.PostgresDatabase(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	repo, err := db.NewPostgresRepo(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repo.Close)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	wire := make(chan *grpcpb.ProfitDealInfo, 1)
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		wire <- request.(*grpcpb.ProfitDealInfo)

		return handler(ctx, request)
	}))
	grpcpb.RegisterExecutorServiceServer(server, receiver.NewGRPCReceiver(core.NewExecutorService(repo)))
	serverDone := make(chan error, 1)

	go func() {
		serverDone <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		listener.Close()

		if err := <-serverDone; err != nil && err != grpc.ErrServerStopped {
			t.Error(err)
		}
	})

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
	})

	detectedAt := time.Date(2026, 10, 13, 10, 11, 12, 123456789, time.FixedZone("fixture", 3*60*60))
	input := contracts.ProfitDealInfo{
		Coin:           "XAUT",
		BuyExchange:    "Bitget",
		SellExchange:   "OKX",
		BuyAmountUSDT:  123.456789012345,
		SellAmountUSDT: 125.987654321098,
		ProfitUSDT:     2.530865308753,
		DetectedAt:     detectedAt,
	}

	if err := sender.NewGRPCSender(conn).Send(ctx, input); err != nil {
		t.Fatal(err)
	}

	var message *grpcpb.ProfitDealInfo

	select {
	case message = <-wire:
	case <-ctx.Done():
		t.Fatal("sender returned without delivering the protobuf signal")
	}

	if message.GetCoin() != input.Coin || message.GetBuyExchange() != input.BuyExchange || message.GetSellExchange() != input.SellExchange ||
		message.GetBuyAmountUsdt() != input.BuyAmountUSDT || message.GetSellAmountUsdt() != input.SellAmountUSDT || message.GetProfitUsdt() != input.ProfitUSDT ||
		message.GetDetectedAt().GetSeconds() != detectedAt.Unix() || message.GetDetectedAt().GetNanos() != int32(detectedAt.Nanosecond()) {
		t.Fatalf("protobuf fields, units or precision changed: %v", message)
	}

	var payload []byte
	table := pgx.Identifier{schema, "profit_deals"}.Sanitize()

	if err := admin.QueryRow(ctx, "SELECT payload FROM "+table).Scan(&payload); err != nil {
		t.Fatal(err)
	}

	var stored contracts.ProfitDealInfo

	if err := json.Unmarshal(payload, &stored); err != nil {
		t.Fatal(err)
	}

	want := input
	want.DetectedAt = detectedAt.UTC()
	if !reflect.DeepEqual(stored, want) {
		t.Fatalf("gRPC receiver or JSONB changed signal fields: %#v, want %#v", stored, want)
	}

	var fields map[string]json.RawMessage

	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}

	for _, field := range []string{"coin", "buy_exchange", "sell_exchange", "buy_amount_usdt", "sell_amount_usdt", "profit_usdt", "detected_at"} {
		if _, exists := fields[field]; !exists {
			t.Errorf("JSONB lost signal contract field %s", field)
		}
	}
}
