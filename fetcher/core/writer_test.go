package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type writerStoreCall struct {
	ctx        context.Context
	at         time.Time
	quotes     []contracts.Quote
	input      []contracts.Quote
	finish     chan error
	returned   chan struct{}
	cancelSeen chan struct{}
}

type controlledWriterStore struct {
	mutex        sync.Mutex
	calls        []*writerStoreCall
	applied      map[bufferTestPair]contracts.Quote
	firstPartial *bufferTestPair
	cancelGate   <-chan struct{}
}

func newControlledWriterStore() *controlledWriterStore {
	return &controlledWriterStore{
		applied: make(map[bufferTestPair]contracts.Quote),
	}
}

func (s *controlledWriterStore) SaveBatch(ctx context.Context, quotes []contracts.Quote) error {
	call := &writerStoreCall{
		ctx:        ctx,
		at:         time.Now(),
		quotes:     append([]contracts.Quote(nil), quotes...),
		input:      quotes,
		finish:     make(chan error, 1),
		returned:   make(chan struct{}),
		cancelSeen: make(chan struct{}),
	}
	s.mutex.Lock()
	s.calls = append(s.calls, call)
	if len(s.calls) == 1 && s.firstPartial != nil {
		for _, quote := range quotes {
			pair := bufferTestPair{
				coin:     quote.Coin,
				exchange: quote.Exchange,
			}
			if pair == *s.firstPartial {
				s.applied[pair] = quote
			}
		}
	}
	s.mutex.Unlock()
	defer close(call.returned)

	select {
	case <-ctx.Done():
		close(call.cancelSeen)
		if s.cancelGate != nil {
			<-s.cancelGate
		}

		return ctx.Err()
	case err := <-call.finish:
		if err != nil {
			return err
		}

		s.mutex.Lock()
		for _, quote := range quotes {
			s.applied[bufferTestPair{
				coin:     quote.Coin,
				exchange: quote.Exchange,
			}] = quote
		}
		s.mutex.Unlock()

		return nil
	}
}

func (s *controlledWriterStore) snapshot() ([]*writerStoreCall, []contracts.Quote) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var applied []contracts.Quote
	for _, quote := range s.applied {
		applied = append(applied, quote)
	}

	return append([]*writerStoreCall(nil), s.calls...), applied
}

func startQuoteWriter(ctx context.Context, buffer *quoteBuffer, store QuoteStore) <-chan struct{} {
	service := NewFetcherService(nil, store)
	done := make(chan struct{})

	go func() {
		defer close(done)

		service.runWriter(ctx, buffer)
	}()

	return done
}

func assertWriterCalls(t *testing.T, store *controlledWriterStore, count int) []*writerStoreCall {
	t.Helper()

	calls, _ := store.snapshot()
	if len(calls) != count {
		t.Fatalf("SaveBatch calls = %d, want %d", len(calls), count)
	}

	return calls
}

func finishWriterCall(t *testing.T, call *writerStoreCall, err error) {
	t.Helper()

	call.finish <- err
	synctest.Wait()
	select {
	case <-call.returned:
	default:
		t.Fatal("SaveBatch did not finish after its result was released")
	}
}

func advanceWriterRetry(t *testing.T, store *controlledWriterStore, previousCalls int) *writerStoreCall {
	t.Helper()

	failedAt := time.Now()
	time.Sleep(100*time.Millisecond - time.Nanosecond)
	synctest.Wait()
	assertWriterCalls(t, store, previousCalls)

	time.Sleep(time.Nanosecond)
	synctest.Wait()
	calls := assertWriterCalls(t, store, previousCalls+1)
	call := calls[previousCalls]
	if call.at.Sub(failedAt) != 100*time.Millisecond {
		t.Fatalf("retry started after %s, want 100 ms", call.at.Sub(failedAt))
	}

	return call
}

func TestWriterRetriesFailedBatchWithoutNewQuotes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		buffer := newQuoteBuffer()
		at := time.Now().UTC()
		quote := bufferTestQuote("ETH", "Binance", "99", at, false)
		invalidation := bufferTestQuote("XAUT", "Bybit", "98", at, true)
		buffer.put(quote)
		buffer.put(invalidation)
		store := newControlledWriterStore()
		done := startQuoteWriter(ctx, buffer, store)

		synctest.Wait()
		call := assertWriterCalls(t, store, 1)[0]
		assertQuoteBatch(t, call.quotes, quote, invalidation)
		for failed := 1; failed <= 3; failed++ {
			finishWriterCall(t, call, errors.New("Redis unavailable"))
			call = advanceWriterRetry(t, store, failed)
			assertQuoteBatch(t, call.quotes, quote, invalidation)
		}

		finishWriterCall(t, call, nil)
		_, applied := store.snapshot()
		assertQuoteBatch(t, applied, quote, invalidation)

		time.Sleep(time.Minute)
		synctest.Wait()
		assertWriterCalls(t, store, 4)
		cancel()
		synctest.Wait()
		assertRetryWorkerStopped(t, done)
	})
}

func TestWriterMergesPendingUpdatesAfterWriteFailure(t *testing.T) {
	for _, partial := range []bool{false, true} {
		for _, oldInvalidated := range []bool{false, true} {
			for _, pendingInvalidated := range []bool{false, true} {
				for _, offset := range []time.Duration{0, time.Millisecond} {
					name := fmt.Sprintf("partial_%v/old_%v/pending_%v/offset_%s", partial, oldInvalidated, pendingInvalidated, offset)
					t.Run(name, func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							ctx, cancel := context.WithCancel(context.Background())
							defer cancel()
							buffer := newQuoteBuffer()
							at := time.Now().UTC()
							old := bufferTestQuote("ETH", "Binance", "99", at, oldInvalidated)
							neighbor := bufferTestQuote("XAUT", "Binance", "98", at, false)
							buffer.put(old)
							buffer.put(neighbor)
							store := newControlledWriterStore()
							if partial {
								store.firstPartial = &bufferTestPair{
									coin:     "ETH",
									exchange: "Binance",
								}
							}
							done := startQuoteWriter(ctx, buffer, store)

							synctest.Wait()
							first := assertWriterCalls(t, store, 1)[0]
							assertQuoteBatch(t, first.quotes, old, neighbor)
							_, applied := store.snapshot()
							if partial {
								assertQuoteBatch(t, applied, old)
							} else {
								assertQuoteBatch(t, applied)
							}

							time.Sleep(offset)
							pending := bufferTestQuote("ETH", "Binance", "103", time.Now().UTC(), pendingInvalidated)
							pending.SourceAt = at.Add(-time.Second)
							other := bufferTestQuote("ETH", "Bybit", "104", time.Now().UTC(), false)
							produced := make(chan struct{})
							go func() {
								buffer.put(pending)
								buffer.put(other)
								close(produced)
							}()

							synctest.Wait()
							select {
							case <-produced:
							default:
								t.Fatal("receiving quotes blocked on the in-flight store operation")
							}
							assertWriterCalls(t, store, 1)
							assertQuoteBatch(t, first.input, old, neighbor)

							finishWriterCall(t, first, errors.New("write failed after a possible partial effect"))
							second := advanceWriterRetry(t, store, 1)
							assertQuoteBatch(t, second.quotes, pending, neighbor, other)
							finishWriterCall(t, second, nil)

							_, applied = store.snapshot()
							assertQuoteBatch(t, applied, pending, neighbor, other)
							time.Sleep(time.Second)
							synctest.Wait()
							assertWriterCalls(t, store, 2)
							cancel()
							synctest.Wait()
							assertRetryWorkerStopped(t, done)
						})
					})
				}
			}
		}
	}
}

func TestWriterCancellationStopsIdleWriteAndRetry(t *testing.T) {
	for _, phase := range []string{"idle", "inflight", "backoff", "already_cancelled", "retry_boundary"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				buffer := newQuoteBuffer()
				store := newControlledWriterStore()
				startedAt := time.Now()
				if phase != "idle" {
					buffer.put(bufferTestQuote("ETH", "Binance", "99", time.Now().UTC(), false))
				}
				if phase == "already_cancelled" {
					cancel()
				}
				done := startQuoteWriter(ctx, buffer, store)

				synctest.Wait()
				if phase == "inflight" || phase == "backoff" || phase == "retry_boundary" {
					call := assertWriterCalls(t, store, 1)[0]
					if phase != "inflight" {
						finishWriterCall(t, call, errors.New("Redis unavailable"))
					}
				}

				wantElapsed := time.Duration(0)
				if phase == "retry_boundary" {
					go func() {
						time.Sleep(100 * time.Millisecond)
						cancel()
					}()

					time.Sleep(100 * time.Millisecond)
					wantElapsed = 100 * time.Millisecond
				} else {
					cancel()
				}

				synctest.Wait()
				assertRetryWorkerStopped(t, done)
				if time.Since(startedAt) != wantElapsed {
					t.Fatalf("cancellation waited unexpectedly: elapsed=%s, want %s", time.Since(startedAt), wantElapsed)
				}

				calls, applied := store.snapshot()
				maxCalls := 1
				if phase == "idle" {
					maxCalls = 0
				} else if phase == "retry_boundary" {
					maxCalls = 2
				}
				if len(calls) > maxCalls || len(applied) != 0 {
					t.Fatalf("cancellation produced unexpected store operations: calls=%d, applied=%#v", len(calls), applied)
				}
				for _, call := range calls {
					if !errors.Is(call.ctx.Err(), context.Canceled) {
						t.Fatal("store context did not receive writer cancellation")
					}
					select {
					case <-call.returned:
					default:
						t.Fatal("writer returned before its store operation completed")
					}
				}

				buffer.put(bufferTestQuote("XAUT", "Bybit", "101", time.Now().UTC(), true))
				time.Sleep(time.Minute)
				synctest.Wait()
				assertWriterCalls(t, store, len(calls))
			})
		})
	}
}
