package core

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type retryQuoteStore struct {
	batches [][]contracts.Quote
	save    func(context.Context, []contracts.Quote) error
}

func (s *retryQuoteStore) SaveBatch(ctx context.Context, quotes []contracts.Quote) error {
	s.batches = append(s.batches, append([]contracts.Quote(nil), quotes...))
	if s.save != nil {
		return s.save(ctx, quotes)
	}

	return nil
}

func startRetryService(ctx context.Context, exchanges []Exchange, store QuoteStore) <-chan struct{} {
	service := NewFetcherService(exchanges, store)
	done := make(chan struct{})

	go func() {
		defer close(done)

		service.Run(ctx, []string{"ETH", "XAUT"})
	}()

	return done
}

func TestServiceKeepsExchangeWorkersIndependent(t *testing.T) {
	for _, failingFirst := range []bool{false, true} {
		name := "survivor_first"
		if failingFirst {
			name = "failing_first"
		}

		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				failing := &retryExchange{
					name: "failing",
				}
				survivor := &retryExchange{
					name: "survivor",
				}
				store := &retryQuoteStore{}
				exchanges := []Exchange{survivor, failing}
				if failingFirst {
					exchanges = []Exchange{failing, survivor}
				}
				done := startRetryService(ctx, exchanges, store)

				synctest.Wait()
				assertRetryAttempts(t, failing, 1)
				assertRetryAttempts(t, survivor, 1)

				for _, delay := range []time.Duration{500 * time.Millisecond, time.Second, 1500 * time.Millisecond, 2 * time.Second} {
					finishRetryAttempt(t, failing)
					advanceRetry(t, failing, delay)
					assertRetryAttempts(t, survivor, 1)
					survivor.snapshot()[0].healthy()
				}

				finishRetryAttempt(t, failing)
				assertRetryAttempts(t, failing, 5)

				select {
				case <-done:
					t.Fatal("Run returned after disabling one exchange while another was active")
				default:
				}

				quote := contracts.Quote{
					Coin:       "ETH",
					Exchange:   survivor.name,
					BidPrice:   "99",
					BidSize:    "2",
					AskPrice:   "100",
					AskSize:    "3",
					ReceivedAt: time.Now(),
				}
				if err := survivor.snapshot()[0].save(quote); err != nil {
					t.Fatal(err)
				}

				synctest.Wait()
				if !reflect.DeepEqual(store.batches, [][]contracts.Quote{{quote}}) {
					t.Fatalf("surviving exchange stopped delivering prices: %#v", store.batches)
				}

				finishRetryAttempt(t, survivor)
				advanceRetry(t, survivor, 500*time.Millisecond)
				assertRetryAttempts(t, failing, 5)
				quote.Coin = "XAUT"
				quote.ReceivedAt = time.Now()
				if err := survivor.snapshot()[1].save(quote); err != nil {
					t.Fatal(err)
				}

				synctest.Wait()
				if len(store.batches) != 2 || !reflect.DeepEqual(store.batches[1], []contracts.Quote{quote}) {
					t.Fatalf("surviving exchange inherited the disabled worker state: %#v", store.batches)
				}

				time.Sleep(time.Hour)
				synctest.Wait()
				assertRetryAttempts(t, failing, 5)
				assertRetryAttempts(t, survivor, 2)

				select {
				case <-done:
					t.Fatal("Run returned before cancellation while the surviving exchange was active")
				default:
				}

				cancel()
				synctest.Wait()
				assertRetryWorkerStopped(t, done)
			})
		})
	}
}

func TestServiceRedisFailuresDoNotChangeExchangeFailureStreak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		exchange := &retryExchange{
			name: "redis-independent",
		}
		remainingFailures := 3
		var delivered []contracts.Quote
		store := &retryQuoteStore{
			save: func(_ context.Context, quotes []contracts.Quote) error {
				if remainingFailures > 0 {
					remainingFailures--

					return errors.New("Redis unavailable")
				}

				delivered = append(delivered, quotes...)

				return nil
			},
		}
		done := startRetryService(ctx, []Exchange{exchange}, store)

		synctest.Wait()
		for _, delay := range []time.Duration{500 * time.Millisecond, time.Second} {
			finishRetryAttempt(t, exchange)
			advanceRetry(t, exchange, delay)
		}

		quote := contracts.Quote{
			Coin:       "ETH",
			Exchange:   exchange.name,
			BidPrice:   "99",
			BidSize:    "2",
			AskPrice:   "100",
			AskSize:    "3",
			ReceivedAt: time.Now(),
		}
		if err := exchange.snapshot()[2].save(quote); err != nil {
			t.Fatalf("Redis error reached the exchange callback: %v", err)
		}

		synctest.Wait()
		for range 3 {
			time.Sleep(100 * time.Millisecond)
			synctest.Wait()
			assertRetryAttempts(t, exchange, 3)
		}

		if len(store.batches) != 4 || remainingFailures != 0 || !reflect.DeepEqual(delivered, []contracts.Quote{quote}) {
			t.Fatalf("writer did not recover independently: attempts=%d, remaining failures=%d, delivered=%#v", len(store.batches), remainingFailures, delivered)
		}

		for _, delay := range []time.Duration{1500 * time.Millisecond, 2 * time.Second} {
			finishRetryAttempt(t, exchange)
			advanceRetry(t, exchange, delay)
		}

		finishRetryAttempt(t, exchange)
		time.Sleep(time.Minute)
		synctest.Wait()
		assertRetryAttempts(t, exchange, 5)

		cancel()
		synctest.Wait()
		assertRetryWorkerStopped(t, done)
	})
}
