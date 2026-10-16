package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/analyzer/core"
	"github.com/fanoxiz/crypto-monitor/contracts"
	fetchercache "github.com/fanoxiz/crypto-monitor/fetcher/adapters/cache"
	"github.com/fanoxiz/crypto-monitor/internal/testsupport"
	"github.com/redis/go-redis/v9"
)

type snapshotHook struct {
	before func(context.Context, redis.Cmder) error
	after  func(context.Context, redis.Cmder) error
}

func (h snapshotHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h snapshotHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		var marker redis.Cmder

		for _, command := range commands {
			if command.Name() == "publish" {
				marker = command
			}
		}

		if marker == nil {
			return next(ctx, commands)
		}

		if h.before != nil {
			if err := h.before(ctx, marker); err != nil {
				return err
			}
		}

		if err := next(ctx, commands); err != nil {
			return err
		}

		if h.after != nil {
			return h.after(ctx, marker)
		}

		return nil
	}
}

func (h snapshotHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		if command.Name() != "eval" && command.Name() != "mget" {
			return next(ctx, command)
		}

		if h.before != nil {
			if err := h.before(ctx, command); err != nil {
				return err
			}
		}

		if err := next(ctx, command); err != nil {
			return err
		}

		if h.after != nil {
			return h.after(ctx, command)
		}

		return nil
	}
}

type snapshotSender struct {
	deals chan contracts.ProfitDealInfo
}

func (s snapshotSender) Send(ctx context.Context, deal contracts.ProfitDealInfo) error {
	select {
	case s.deals <- deal:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func quoteStreamClients(t *testing.T) (*redis.Client, *redis.Client) {
	t.Helper()

	client := testsupport.RedisClient(t, 3)
	publisher := redis.NewClient(&redis.Options{
		Addr: client.Options().Addr,
		DB:   client.Options().DB,
	})
	t.Cleanup(func() {
		publisher.Close()
	})

	return client, publisher
}

func snapshotQuote(coin, exchange, bid, ask string) contracts.Quote {
	now := time.Now().UTC().Add(-time.Millisecond)

	return contracts.Quote{
		Coin:       coin,
		Exchange:   exchange,
		BidPrice:   bid,
		BidSize:    "1",
		AskPrice:   ask,
		AskSize:    "1",
		SourceAt:   now,
		ReceivedAt: now,
	}
}

func TestStreamSnapshotDoesNotResurrectInvalidatedQuote(t *testing.T) {
	for _, fixture := range []struct {
		name        string
		invalidated bool
	}{
		{
			name:        "invalidated_before_snapshot",
			invalidated: true,
		},
		{
			name: "coexisting_prices",
		},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			client, publisher := quoteStreamClients(t)
			writer := fetchercache.NewRedisQuoteStore(publisher, 10*time.Second)
			client.AddHook(snapshotHook{
				before: func(ctx context.Context, command redis.Cmder) error {
					buy := snapshotQuote("SNAPSHOT", "Bybit", "99", "100")

					if err := writer.SaveBatch(ctx, []contracts.Quote{buy}); err != nil {
						return err
					}

					if fixture.invalidated {
						buy.Invalidated = true
						buy.ReceivedAt = time.Now().UTC()

						if err := writer.SaveBatch(ctx, []contracts.Quote{buy}); err != nil {
							return err
						}
					}

					return writer.SaveBatch(ctx, []contracts.Quote{snapshotQuote("SNAPSHOT", "OKX", "105", "106")})
				},
				after: func(ctx context.Context, command redis.Cmder) error {
					return writer.SaveBatch(ctx, []contracts.Quote{
						snapshotQuote("SNAPSHOTCONTROL", "Bybit", "199", "200"),
						snapshotQuote("SNAPSHOTCONTROL", "OKX", "207", "208"),
					})
				},
			})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			sender := snapshotSender{
				deals: make(chan contracts.ProfitDealInfo, 16),
			}
			store := NewRedisQuoteStore(client, []string{"SNAPSHOT", "SNAPSHOTCONTROL"}, []string{"Bybit", "OKX"})
			service := core.NewAnalyzerService(map[string]float64{
				"Bybit": 0,
				"OKX":   0,
			}, store, sender, 10*time.Second)
			done := make(chan struct{})

			go func() {
				defer close(done)

				service.Run(ctx)
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})

			var signals []contracts.ProfitDealInfo

			for {
				select {
				case deal := <-sender.deals:
					if deal.Coin == "SNAPSHOTCONTROL" {
						if deal.ProfitUSDT != 7 {
							t.Fatalf("control profit = %v, want 7", deal.ProfitUSDT)
						}

						if fixture.invalidated {
							if len(signals) != 0 {
								t.Fatalf("snapshot resurrected an invalidated price: %#v", signals)
							}
						} else if len(signals) != 1 || signals[0].ProfitUSDT != 5 || signals[0].BuyExchange != "Bybit" || signals[0].SellExchange != "OKX" {
							t.Fatalf("coexisting prices produced %#v, want one Bybit/OKX signal with profit 5", signals)
						}

						return
					}

					signals = append(signals, deal)
				case <-ctx.Done():
					t.Fatal("analyzer did not deliver the control signal")
				}
			}
		})
	}
}

func TestStreamWaitsForItsOwnSnapshotMarker(t *testing.T) {
	client, publisher := quoteStreamClients(t)
	var marker string
	client.AddHook(snapshotHook{
		before: func(ctx context.Context, command redis.Cmder) error {
			args := command.Args()
			marker = args[len(args)-1].(string)
			args[len(args)-1] = `{"snapshot":"another-subscriber"}`

			return nil
		},
		after: func(ctx context.Context, command redis.Cmder) error {
			for _, value := range []any{
				snapshotQuote("SNAPSHOT", "Bybit", "99", "100"),
				json.RawMessage(marker),
				snapshotQuote("SNAPSHOT", "Bybit", "119", "120"),
			} {
				payload, err := json.Marshal(value)
				if err != nil {
					return err
				}

				if err := publisher.Publish(ctx, contracts.QuotesChannel, payload).Err(); err != nil {
					return err
				}
			}

			return nil
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	quotes := make(chan contracts.Quote, 4)
	done := make(chan error, 1)
	store := NewRedisQuoteStore(client, []string{"SNAPSHOT"}, []string{"Bybit"})

	go func() {
		done <- store.Stream(ctx, func(quote contracts.Quote) {
			quotes <- quote
		})
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	select {
	case quote := <-quotes:
		if quote.AskPrice != "120" {
			t.Fatalf("quote before the matching snapshot marker was replayed: %#v", quote)
		}
	case <-ctx.Done():
		t.Fatal("live quote after the matching snapshot marker was lost")
	}
}

func TestStreamSnapshotFailureEmitsNoQuotes(t *testing.T) {
	for _, phase := range []string{"before_snapshot", "after_snapshot"} {
		t.Run(phase, func(t *testing.T) {
			client, publisher := quoteStreamClients(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if err := fetchercache.NewRedisQuoteStore(publisher, 10*time.Second).SaveBatch(ctx, []contracts.Quote{
				snapshotQuote("SNAPSHOT", "Bybit", "99", "100"),
			}); err != nil {
				t.Fatal(err)
			}

			failure := errors.New("snapshot connection failed")
			hook := snapshotHook{}
			fail := func(ctx context.Context, command redis.Cmder) error {
				return failure
			}

			if phase == "before_snapshot" {
				hook.before = fail
			} else {
				hook.after = fail
			}

			client.AddHook(hook)
			store := NewRedisQuoteStore(client, []string{"SNAPSHOT"}, []string{"Bybit"})
			err := store.Stream(ctx, func(quote contracts.Quote) {
				t.Errorf("failed snapshot emitted a quote: %#v", quote)
			})
			if !errors.Is(err, failure) {
				t.Fatalf("stream error = %v, want snapshot failure", err)
			}
		})
	}
}

func TestStreamMissingSnapshotMarkerStopsOnCancellationOrTimeout(t *testing.T) {
	for _, interrupted := range []bool{true, false} {
		name := "timeout"
		if interrupted {
			name = "cancellation"
		}

		t.Run(name, func(t *testing.T) {
			client, _ := quoteStreamClients(t)
			ready := make(chan struct{})
			client.AddHook(snapshotHook{
				before: func(ctx context.Context, command redis.Cmder) error {
					args := command.Args()
					args[len(args)-1] = `{"snapshot":"another-subscriber"}`

					return nil
				},
				after: func(ctx context.Context, command redis.Cmder) error {
					close(ready)

					return nil
				},
			})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			store := NewRedisQuoteStore(client, []string{"SNAPSHOT"}, []string{"Bybit"})
			done := make(chan error, 1)

			go func() {
				defer close(done)

				done <- store.Stream(ctx, func(quote contracts.Quote) {
					t.Errorf("incomplete snapshot emitted a quote: %#v", quote)
				})
			}()
			t.Cleanup(func() {
				cancel()
				<-done
			})

			select {
			case <-ready:
			case <-ctx.Done():
				t.Fatal("snapshot command did not complete")
			}

			want := context.DeadlineExceeded
			if interrupted {
				want = context.Canceled
				cancel()
			}

			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("stream error = %v, want %v", err, want)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("stream remained blocked waiting for a missing snapshot marker")
			}
		})
	}
}

func TestSnapshotRejectsSubscriptionReset(t *testing.T) {
	client, _ := quoteStreamClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pubsub := client.Subscribe(ctx, contracts.QuotesChannel)
	defer pubsub.Close()

	if err := pubsub.Subscribe(ctx, contracts.QuotesChannel); err != nil {
		t.Fatal(err)
	}

	store := NewRedisQuoteStore(client, []string{"SNAPSHOT"}, []string{"Bybit"})
	quotes, err := store.loadSnapshot(ctx, pubsub)
	if err == nil || errors.Is(err, context.DeadlineExceeded) || len(quotes) != 0 {
		t.Fatalf("subscription reset returned snapshot %#v, error %v", quotes, err)
	}
}

type snapshotReplyLossConn struct {
	net.Conn
	drop      atomic.Bool
	snapshots *atomic.Int32
}

func (c *snapshotReplyLossConn) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte("\r\nmget\r\n")) {
		c.drop.Store(true)
		c.snapshots.Add(1)
	}

	return c.Conn.Write(data)
}

func (c *snapshotReplyLossConn) Read(data []byte) (int, error) {
	count, err := c.Conn.Read(data)
	if count > 0 && c.drop.Swap(false) {
		return 0, io.ErrUnexpectedEOF
	}

	return count, err
}

func TestStreamDoesNotRetrySnapshotAfterLosingItsReply(t *testing.T) {
	_, publisher := quoteStreamClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	witness := publisher.Subscribe(ctx, contracts.QuotesChannel)
	defer witness.Close()

	stop := context.AfterFunc(ctx, func() {
		witness.Close()
	})
	defer stop()

	if _, err := witness.Receive(ctx); err != nil {
		t.Fatal(err)
	}

	var snapshots atomic.Int32
	client := redis.NewClient(&redis.Options{
		Addr:     publisher.Options().Addr,
		DB:       publisher.Options().DB,
		Protocol: 2,
		Dialer: func(ctx context.Context, network, address string) (net.Conn, error) {
			connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}

			return &snapshotReplyLossConn{
				Conn:      connection,
				snapshots: &snapshots,
			}, nil
		},
	})
	t.Cleanup(func() {
		client.Close()
	})

	var marker string
	client.AddHook(snapshotHook{
		before: func(ctx context.Context, command redis.Cmder) error {
			args := command.Args()
			marker = args[len(args)-1].(string)

			return nil
		},
	})
	store := NewRedisQuoteStore(client, []string{"SNAPSHOT"}, []string{"Bybit"})
	err := store.Stream(ctx, func(quote contracts.Quote) {
		t.Errorf("snapshot with a lost reply emitted a quote: %#v", quote)
	})
	if err == nil || snapshots.Load() != 1 {
		t.Fatalf("snapshot executions = %d, error %v; want one failed attempt", snapshots.Load(), err)
	}

	for {
		message, err := witness.ReceiveMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if message.Payload == marker {
			return
		}
	}
}
