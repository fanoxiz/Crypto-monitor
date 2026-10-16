package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gorilla/websocket"
)

type heartbeatLoopObservation struct {
	received chan time.Time
	failed   chan error
	done     chan struct{}
	healthy  int
}

func observeHeartbeatLoop(ctx context.Context, conn *websocket.Conn, packet string, received chan time.Time) *heartbeatLoopObservation {
	observation := &heartbeatLoopObservation{
		received: received,
		failed:   make(chan error, 1),
		done:     make(chan struct{}),
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	go func() {
		defer close(observation.done)

		runHeartbeat(ctx, conn, logger, packet, received, observation.failed, func() {
			observation.healthy++
		})
	}()

	return observation
}

func assertHeartbeatLoopStopped(t *testing.T, observation *heartbeatLoopObservation, healthy int) {
	t.Helper()

	select {
	case <-observation.done:
	default:
		t.Fatal("heartbeat loop remained active")
	}

	if observation.healthy != healthy {
		t.Fatalf("healthy callbacks = %d, want %d", observation.healthy, healthy)
	}
}

func TestHeartbeatLoopPolicyAndQueuedReply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, peer, closeTransport := heartbeatSockets(t)
		defer closeTransport()
		packets, _ := observeHeartbeatPackets(peer)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		startedAt := time.Now()
		received := make(chan time.Time, 1)
		received <- startedAt
		observation := observeHeartbeatLoop(ctx, client, "ping", received)

		synctest.Wait()
		first := takeHeartbeatPacket(t, packets)
		if first.kind != websocket.TextMessage || first.data != "ping" || !first.at.Equal(startedAt) {
			t.Fatalf("initial heartbeat = %#v, want immediate text ping", first)
		}

		if observation.healthy != 0 {
			t.Fatal("reply queued before sending heartbeat confirmed the new check")
		}

		time.Sleep(2 * time.Second)
		received <- time.Now()
		synctest.Wait()
		if observation.healthy != 1 {
			t.Fatalf("fresh reply produced %d healthy callbacks, want one", observation.healthy)
		}

		time.Sleep(18*time.Second - time.Nanosecond)
		synctest.Wait()
		select {
		case packet := <-packets:
			t.Fatalf("periodic heartbeat arrived before 20 seconds: %#v", packet)
		default:
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		second := takeHeartbeatPacket(t, packets)
		if second.kind != websocket.TextMessage || second.data != "ping" || second.at.Sub(startedAt) != 20*time.Second {
			t.Fatalf("periodic heartbeat = %#v, want ping at 20 seconds", second)
		}

		if observation.healthy != 1 {
			t.Fatal("sending a heartbeat confirmed health without a response")
		}

		received <- time.Now()
		synctest.Wait()
		cancel()
		synctest.Wait()
		assertHeartbeatLoopStopped(t, observation, 2)

		time.Sleep(time.Minute)
		synctest.Wait()
		assertHeartbeatLoopStopped(t, observation, 2)
		select {
		case packet := <-packets:
			t.Fatalf("heartbeat sent after loop stopped: %#v", packet)
		case err := <-observation.failed:
			t.Fatalf("cancellation reported a connection failure: %v", err)
		default:
		}
	})
}

func TestHeartbeatLoopTimeoutAndLateReply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, peer, closeTransport := heartbeatSockets(t)
		defer closeTransport()
		packets, _ := observeHeartbeatPackets(peer)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		received := make(chan time.Time, 1)
		observation := observeHeartbeatLoop(ctx, client, "ping", received)

		synctest.Wait()
		takeHeartbeatPacket(t, packets)
		time.Sleep(5*time.Second - time.Nanosecond)
		synctest.Wait()
		select {
		case <-observation.done:
			t.Fatal("heartbeat timed out before five seconds")
		case err := <-observation.failed:
			t.Fatalf("heartbeat failed early: %v", err)
		default:
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		assertHeartbeatLoopStopped(t, observation, 0)
		select {
		case err := <-observation.failed:
			if err == nil || ctx.Err() != nil {
				t.Fatalf("heartbeat timeout = %v, context error = %v", err, ctx.Err())
			}
		default:
			t.Fatal("missing heartbeat response produced no failure")
		}

		received <- time.Now()
		time.Sleep(time.Minute)
		synctest.Wait()
		assertHeartbeatLoopStopped(t, observation, 0)
		select {
		case packet := <-packets:
			t.Fatalf("late reply restarted heartbeat: %#v", packet)
		case err := <-observation.failed:
			t.Fatalf("stopped loop reported another failure: %v", err)
		default:
		}
	})
}

func TestHeartbeatLoopWriteFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, _, closeTransport := heartbeatSockets(t)
		defer closeTransport()
		client.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		observation := observeHeartbeatLoop(ctx, client, "ping", make(chan time.Time, 1))

		synctest.Wait()
		assertHeartbeatLoopStopped(t, observation, 0)
		select {
		case err := <-observation.failed:
			if !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
				t.Fatalf("write failure lost the transport cause: %v", err)
			}
		default:
			t.Fatal("failed heartbeat write produced no connection failure")
		}
	})
}

func TestHeartbeatLoopBlockedWriteDeadline(t *testing.T) {
	for name, packet := range map[string]string{
		"text":   "ping",
		"native": "",
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, _, closeTransport := heartbeatSockets(t)
				defer closeTransport()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				startedAt := time.Now()
				observation := observeHeartbeatLoop(ctx, client, packet, make(chan time.Time, 1))

				synctest.Wait()
				time.Sleep(5*time.Second - time.Nanosecond)
				synctest.Wait()

				select {
				case <-observation.done:
					t.Fatal("blocked heartbeat write ended before five seconds")
				case err := <-observation.failed:
					t.Fatalf("blocked heartbeat write failed early: %v", err)
				default:
				}

				if observation.healthy != 0 {
					t.Fatal("blocked heartbeat write confirmed health")
				}

				time.Sleep(time.Nanosecond)
				synctest.Wait()
				assertHeartbeatLoopStopped(t, observation, 0)

				select {
				case err := <-observation.failed:
					var networkErr net.Error
					if !errors.As(err, &networkErr) || !networkErr.Timeout() {
						t.Fatalf("blocked heartbeat write lost its timeout cause: %v", err)
					}
				default:
					t.Fatal("blocked heartbeat write produced no connection failure")
				}

				if ctx.Err() != nil || time.Since(startedAt) != 5*time.Second {
					t.Fatalf("write deadline ended after %s with context error %v", time.Since(startedAt), ctx.Err())
				}
			})
		})
	}
}

func TestHeartbeatLoopCancellation(t *testing.T) {
	for _, phase := range []string{"before_start", "waiting_reply", "between_checks"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, peer, closeTransport := heartbeatSockets(t)
				defer closeTransport()
				packets, _ := observeHeartbeatPackets(peer)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if phase == "before_start" {
					cancel()
				}
				received := make(chan time.Time, 1)
				observation := observeHeartbeatLoop(ctx, client, "ping", received)

				synctest.Wait()
				healthy := 0
				if phase != "before_start" {
					takeHeartbeatPacket(t, packets)
				}

				if phase == "between_checks" {
					received <- time.Now()
					synctest.Wait()
					healthy = 1
				}

				cancel()
				synctest.Wait()
				assertHeartbeatLoopStopped(t, observation, healthy)
				received <- time.Now()
				time.Sleep(time.Minute)
				synctest.Wait()
				assertHeartbeatLoopStopped(t, observation, healthy)

				select {
				case packet := <-packets:
					t.Fatalf("cancelled loop sent a heartbeat: %#v", packet)
				case err := <-observation.failed:
					t.Fatalf("cancelled loop reported a connection failure: %v", err)
				default:
				}
			})
		})
	}
}
