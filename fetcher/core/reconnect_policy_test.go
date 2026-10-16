package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type retryAttempt struct {
	at       time.Time
	coins    []string
	save     func(contracts.Quote) error
	healthy  func()
	finish   chan error
	returned chan struct{}
}

type retryExchange struct {
	name     string
	mutex    sync.Mutex
	attempts []*retryAttempt
}

func (e *retryExchange) Name() string {
	return e.name
}

func (e *retryExchange) snapshot() []*retryAttempt {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	return append([]*retryAttempt(nil), e.attempts...)
}

func (e *retryExchange) Stream(ctx context.Context, coins []string, save func(contracts.Quote) error, onHealthy func()) error {
	attempt := &retryAttempt{
		at:       time.Now(),
		coins:    append([]string(nil), coins...),
		save:     save,
		healthy:  onHealthy,
		finish:   make(chan error, 1),
		returned: make(chan struct{}),
	}
	e.mutex.Lock()
	e.attempts = append(e.attempts, attempt)
	e.mutex.Unlock()

	defer close(attempt.returned)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-attempt.finish:
		return err
	}
}

func startRetryWorker(ctx context.Context, exchange *retryExchange, save func(contracts.Quote) error) <-chan struct{} {
	if save == nil {
		save = func(contracts.Quote) error {
			return nil
		}
	}

	service := NewFetcherService(nil, nil)
	done := make(chan struct{})

	go func() {
		defer close(done)

		service.runExchange(ctx, exchange, []string{"ETH", "XAUT"}, save)
	}()

	return done
}

func assertRetryAttempts(t *testing.T, exchange *retryExchange, count int) {
	t.Helper()

	attempts := exchange.snapshot()
	if len(attempts) != count {
		t.Fatalf("%s attempts = %d, want %d", exchange.name, len(attempts), count)
	}

	for _, attempt := range attempts {
		if !reflect.DeepEqual(attempt.coins, []string{"ETH", "XAUT"}) {
			t.Fatalf("retry changed requested coins: %v", attempt.coins)
		}
	}
}

func finishRetryAttempt(t *testing.T, exchange *retryExchange) {
	t.Helper()

	attempts := exchange.snapshot()
	attempt := attempts[len(attempts)-1]
	attempt.finish <- errors.New("exchange transport failed")
	synctest.Wait()

	select {
	case <-attempt.returned:
	default:
		t.Fatal("exchange attempt did not finish")
	}
}

func advanceRetry(t *testing.T, exchange *retryExchange, delay time.Duration) {
	t.Helper()

	count := len(exchange.snapshot())
	startedAt := time.Now()
	time.Sleep(delay - time.Nanosecond)
	synctest.Wait()
	assertRetryAttempts(t, exchange, count)

	time.Sleep(time.Nanosecond)
	synctest.Wait()
	assertRetryAttempts(t, exchange, count+1)

	if at := exchange.snapshot()[count].at; !at.Equal(startedAt.Add(delay)) {
		t.Fatalf("retry started at %s, want %s", at, startedAt.Add(delay))
	}
}

func assertRetryWorkerStopped(t *testing.T, done <-chan struct{}) {
	t.Helper()

	select {
	case <-done:
	default:
		t.Fatal("exchange worker remained active")
	}
}

func TestExchangeRetryPolicy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		exchange := &retryExchange{
			name: "failing",
		}
		startedAt := time.Now()
		done := startRetryWorker(ctx, exchange, nil)

		synctest.Wait()
		assertRetryAttempts(t, exchange, 1)
		if !exchange.snapshot()[0].at.Equal(startedAt) {
			t.Fatal("initial connection attempt was delayed")
		}

		for _, delay := range []time.Duration{500 * time.Millisecond, time.Second, 1500 * time.Millisecond, 2 * time.Second} {
			finishRetryAttempt(t, exchange)
			advanceRetry(t, exchange, delay)
		}

		finishRetryAttempt(t, exchange)
		assertRetryWorkerStopped(t, done)
		if ctx.Err() != nil || time.Since(startedAt) != 5*time.Second {
			t.Fatalf("fifth failure stopped after %s, context error = %v", time.Since(startedAt), ctx.Err())
		}

		time.Sleep(time.Hour)
		synctest.Wait()
		assertRetryAttempts(t, exchange, 5)
	})
}

func TestExchangeHeartbeatResetsFailureStreak(t *testing.T) {
	for name, heartbeats := range map[string]int{
		"single":   1,
		"repeated": 2,
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				exchange := &retryExchange{
					name: "recovering",
				}
				done := startRetryWorker(ctx, exchange, nil)

				synctest.Wait()
				for _, delay := range []time.Duration{500 * time.Millisecond, time.Second} {
					finishRetryAttempt(t, exchange)
					advanceRetry(t, exchange, delay)
				}

				for range heartbeats {
					exchange.snapshot()[2].healthy()
				}

				synctest.Wait()
				assertRetryAttempts(t, exchange, 3)

				for _, delay := range []time.Duration{500 * time.Millisecond, time.Second, 1500 * time.Millisecond, 2 * time.Second} {
					finishRetryAttempt(t, exchange)
					advanceRetry(t, exchange, delay)
				}

				finishRetryAttempt(t, exchange)
				assertRetryWorkerStopped(t, done)
				assertRetryAttempts(t, exchange, 7)

				time.Sleep(time.Minute)
				synctest.Wait()
				assertRetryAttempts(t, exchange, 7)
			})
		})
	}
}

func TestExchangeQuotesDoNotResetFailureStreak(t *testing.T) {
	for _, invalidated := range []bool{false, true} {
		name := "quote"
		if invalidated {
			name = "invalidation"
		}

		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				exchange := &retryExchange{
					name: "market-data-only",
				}
				var quotes []contracts.Quote
				done := startRetryWorker(ctx, exchange, func(quote contracts.Quote) error {
					quotes = append(quotes, quote)

					return nil
				})

				synctest.Wait()
				for _, delay := range []time.Duration{500 * time.Millisecond, time.Second} {
					finishRetryAttempt(t, exchange)
					advanceRetry(t, exchange, delay)
				}

				quote := contracts.Quote{
					Coin:        "ETH",
					Exchange:    exchange.name,
					BidPrice:    "99",
					BidSize:     "2",
					AskPrice:    "100",
					AskSize:     "3",
					ReceivedAt:  time.Now(),
					Invalidated: invalidated,
				}
				if invalidated {
					quote.BidPrice = ""
					quote.BidSize = ""
					quote.AskPrice = ""
					quote.AskSize = ""
				}

				if err := exchange.snapshot()[2].save(quote); err != nil {
					t.Fatal(err)
				}

				if !reflect.DeepEqual(quotes, []contracts.Quote{quote}) {
					t.Fatalf("market event did not reach the callback: %#v", quotes)
				}

				for _, delay := range []time.Duration{1500 * time.Millisecond, 2 * time.Second} {
					finishRetryAttempt(t, exchange)
					advanceRetry(t, exchange, delay)
				}

				finishRetryAttempt(t, exchange)
				assertRetryWorkerStopped(t, done)
				assertRetryAttempts(t, exchange, 5)
			})
		})
	}
}

func TestExchangeCancellationStopsRetries(t *testing.T) {
	for _, phase := range []string{"before_start", "active", "active_fifth", "backoff_1", "backoff_2", "backoff_3", "backoff_4"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var logs bytes.Buffer
				previousLogger := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
					Level: slog.LevelWarn,
				})))
				defer slog.SetDefault(previousLogger)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				exchange := &retryExchange{
					name: "cancelled",
				}
				if phase == "before_start" {
					cancel()
				}
				done := startRetryWorker(ctx, exchange, nil)

				synctest.Wait()
				failures := 0
				if phase == "active_fifth" {
					for _, delay := range []time.Duration{500 * time.Millisecond, time.Second, 1500 * time.Millisecond, 2 * time.Second} {
						finishRetryAttempt(t, exchange)
						advanceRetry(t, exchange, delay)
						failures++
					}
				}

				for index, name := range []string{"backoff_1", "backoff_2", "backoff_3", "backoff_4"} {
					if phase != name {
						continue
					}

					for failure := 1; failure <= index+1; failure++ {
						finishRetryAttempt(t, exchange)
						failures++
						if failure <= index {
							advanceRetry(t, exchange, time.Duration(failure)*500*time.Millisecond)
						}
					}
				}

				attempts := len(exchange.snapshot())
				cancelledAt := time.Now()
				cancel()
				synctest.Wait()
				assertRetryWorkerStopped(t, done)
				if !time.Now().Equal(cancelledAt) {
					t.Fatal("cancellation waited for a retry timer")
				}

				decoder := json.NewDecoder(&logs)
				var recorded []int
				for {
					var record struct {
						Failures int `json:"failures"`
					}
					err := decoder.Decode(&record)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}

					recorded = append(recorded, record.Failures)
				}

				if len(recorded) != failures {
					t.Fatalf("recorded failures = %v, want %d failures before cancellation", recorded, failures)
				}

				for index, count := range recorded {
					if count != index+1 {
						t.Fatalf("failure counted more than once: %v", recorded)
					}
				}

				time.Sleep(time.Minute)
				synctest.Wait()
				assertRetryAttempts(t, exchange, attempts)
			})
		})
	}
}
