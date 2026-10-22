package core

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

func deliverWriterQuotes(t *testing.T, attempt *retryAttempt, quotes ...contracts.Quote) {
	t.Helper()

	result := make(chan error, 1)
	go func() {
		for _, quote := range quotes {
			if err := attempt.save(quote); err != nil {
				result <- err

				return
			}
		}

		result <- nil
	}()

	synctest.Wait()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("quote callback returned a store failure: %v", err)
		}
	default:
		t.Fatal("quote callback blocked on the store operation")
	}
}

func TestServiceBuffersQuotesWhileStoreIsBlocked(t *testing.T) {
	for name, firstError := range map[string]error{
		"success": nil,
		"failure": errors.New("Redis unavailable"),
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				exchange := &retryExchange{
					name: "Binance",
				}
				store := newControlledWriterStore()
				done := startRetryService(ctx, []Exchange{exchange}, store)

				synctest.Wait()
				assertRetryAttempts(t, exchange, 1)
				attempt := exchange.snapshot()[0]
				at := time.Now().UTC()
				old := bufferTestQuote("ETH", "Binance", "99", at, false)
				deliverWriterQuotes(t, attempt, old)
				first := assertWriterCalls(t, store, 1)[0]
				assertQuoteBatch(t, first.quotes, old)

				intermediate := bufferTestQuote("ETH", "Binance", "100", at, false)
				invalidation := bufferTestQuote("ETH", "Binance", "", at, true)
				latest := bufferTestQuote("ETH", "Binance", "102", at, false)
				neighbor := bufferTestQuote("XAUT", "Binance", "88", at, false)
				deliverWriterQuotes(t, attempt, intermediate, invalidation, latest, neighbor)
				assertWriterCalls(t, store, 1)
				assertQuoteBatch(t, first.input, old)
				select {
				case <-done:
					t.Fatal("Run returned while the writer and exchange were active")
				default:
				}

				finishWriterCall(t, first, firstError)
				var second *writerStoreCall
				if firstError == nil {
					second = assertWriterCalls(t, store, 2)[1]
				} else {
					second = advanceWriterRetry(t, store, 1)
				}

				assertQuoteBatch(t, second.quotes, latest, neighbor)
				finishWriterCall(t, second, nil)
				_, applied := store.snapshot()
				assertQuoteBatch(t, applied, latest, neighbor)
				assertRetryAttempts(t, exchange, 1)

				select {
				case <-done:
					t.Fatal("Run returned while an exchange was still active")
				default:
				}

				cancel()
				synctest.Wait()
				assertRetryWorkerStopped(t, done)
				time.Sleep(time.Minute)
				synctest.Wait()
				assertWriterCalls(t, store, 2)
			})
		})
	}
}

func TestServiceWaitsForStoreCancellationToFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		gate := make(chan struct{})
		defer func() {
			if gate != nil {
				close(gate)
			}
		}()
		exchange := &retryExchange{
			name: "Binance",
		}
		store := newControlledWriterStore()
		store.cancelGate = gate
		done := startRetryService(ctx, []Exchange{exchange}, store)

		synctest.Wait()
		attempt := exchange.snapshot()[0]
		quote := bufferTestQuote("ETH", "Binance", "99", time.Now().UTC(), false)
		deliverWriterQuotes(t, attempt, quote)
		call := assertWriterCalls(t, store, 1)[0]

		cancel()
		synctest.Wait()
		select {
		case <-call.cancelSeen:
		default:
			t.Fatal("cancellation did not reach the in-flight store operation")
		}
		select {
		case <-attempt.returned:
		default:
			t.Fatal("exchange did not stop on cancellation")
		}
		select {
		case <-done:
			t.Fatal("Run returned before the store finished cancelling")
		default:
		}

		close(gate)
		gate = nil
		synctest.Wait()
		assertRetryWorkerStopped(t, done)
		select {
		case <-call.returned:
		default:
			t.Fatal("store operation outlived Run")
		}

		_, applied := store.snapshot()
		assertQuoteBatch(t, applied)
		time.Sleep(time.Minute)
		synctest.Wait()
		assertWriterCalls(t, store, 1)
	})
}
