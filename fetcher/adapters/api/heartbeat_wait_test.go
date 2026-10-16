package api

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestWaitHeartbeatAcceptsResponseWindow(t *testing.T) {
	for name, offset := range map[string]time.Duration{
		"at_send":         0,
		"before_deadline": 5*time.Second - time.Nanosecond,
		"at_deadline":     5 * time.Second,
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sentAt := time.Now()
				received := make(chan time.Time, 1)
				received <- sentAt.Add(offset)

				if err := waitHeartbeat(context.Background(), received, sentAt); err != nil {
					t.Fatalf("in-window heartbeat rejected: %v", err)
				}
			})
		})
	}
}

func TestWaitHeartbeatIgnoresStaleResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sentAt := time.Now()
		received := make(chan time.Time, 1)
		received <- sentAt.Add(-time.Nanosecond)
		done := make(chan error, 1)

		go func() {
			done <- waitHeartbeat(ctx, received, sentAt)
		}()

		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("stale response ended heartbeat wait: %v", err)
		default:
		}

		time.Sleep(4 * time.Second)
		received <- time.Now().In(time.FixedZone("response", 3*60*60))
		synctest.Wait()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("fresh response after stale one rejected: %v", err)
			}
		default:
			t.Fatal("fresh response did not finish heartbeat wait")
		}
	})
}

func TestWaitHeartbeatStaleResponseDoesNotExtendTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		startedAt := time.Now()
		received := make(chan time.Time, 1)
		done := make(chan error, 1)

		go func() {
			done <- waitHeartbeat(ctx, received, startedAt)
		}()

		synctest.Wait()
		time.Sleep(4 * time.Second)
		received <- startedAt.Add(-time.Nanosecond)
		synctest.Wait()

		select {
		case err := <-done:
			t.Fatalf("stale response ended heartbeat wait: %v", err)
		default:
		}

		time.Sleep(time.Second - time.Nanosecond)
		synctest.Wait()

		select {
		case err := <-done:
			t.Fatalf("heartbeat wait ended before its original deadline: %v", err)
		default:
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()

		select {
		case err := <-done:
			if err == nil || ctx.Err() != nil || time.Since(startedAt) != 5*time.Second {
				t.Fatalf("heartbeat wait result = %v after %s, context error = %v", err, time.Since(startedAt), ctx.Err())
			}
		default:
			t.Fatal("stale response extended the original five-second heartbeat deadline")
		}
	})
}

func TestWaitHeartbeatRejectsTimestampAfterDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sentAt := time.Now()
		received := make(chan time.Time, 1)
		received <- sentAt.Add(5*time.Second + time.Nanosecond)

		if err := waitHeartbeat(context.Background(), received, sentAt); err == nil {
			t.Fatal("response timestamp beyond the five-second window was accepted")
		}
	})
}

func TestWaitHeartbeatFiveSecondTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		startedAt := time.Now()
		received := make(chan time.Time, 1)
		done := make(chan error, 1)

		go func() {
			done <- waitHeartbeat(ctx, received, startedAt)
		}()

		synctest.Wait()
		time.Sleep(5*time.Second - time.Nanosecond)
		synctest.Wait()

		select {
		case err := <-done:
			t.Fatalf("heartbeat wait ended before five seconds: %v", err)
		default:
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()

		select {
		case err := <-done:
			if err == nil || ctx.Err() != nil {
				t.Fatalf("no-response result = %v, want timeout without context cancellation", err)
			}
		default:
			t.Fatal("heartbeat wait did not end at five seconds")
		}
	})
}

func TestWaitHeartbeatCancellation(t *testing.T) {
	for _, pending := range []bool{false, true} {
		name := "already_cancelled"
		if pending {
			name = "pending_wait"
		}

		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				startedAt := time.Now()
				received := make(chan time.Time, 1)
				done := make(chan error, 1)
				if !pending {
					cancel()
				}

				go func() {
					done <- waitHeartbeat(ctx, received, startedAt)
				}()

				synctest.Wait()
				cancel()
				synctest.Wait()

				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) || time.Since(startedAt) != 0 {
						t.Fatalf("cancelled wait result = %v after %s", err, time.Since(startedAt))
					}
				default:
					t.Fatal("cancelled wait remained blocked")
				}
			})
		})
	}
}

func TestWaitHeartbeatConcurrentDeadlineEvents(t *testing.T) {
	for _, cancelAtBoundary := range []bool{false, true} {
		name := "reply_and_timer"
		if cancelAtBoundary {
			name = "reply_timer_and_cancel"
		}

		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				startedAt := time.Now()
				received := make(chan time.Time, 1)
				done := make(chan error, 1)

				go func() {
					done <- waitHeartbeat(ctx, received, startedAt)
				}()

				synctest.Wait()

				go func() {
					time.Sleep(5 * time.Second)
					received <- time.Now()
					if cancelAtBoundary {
						cancel()
					}
				}()

				err := <-done
				synctest.Wait()

				if elapsed := time.Since(startedAt); elapsed != 5*time.Second {
					t.Fatalf("deadline race ended at %s, want five seconds; error: %v", elapsed, err)
				}

				if !cancelAtBoundary && errors.Is(err, context.Canceled) {
					t.Fatal("wait invented cancellation")
				}
			})
		})
	}
}
