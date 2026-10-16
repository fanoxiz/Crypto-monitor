package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/gorilla/websocket"
)

type heartbeatStreamObservation struct {
	peer           *websocket.Conn
	ctx            context.Context
	cancel         context.CancelFunc
	packets        <-chan heartbeatPacket
	readerDone     <-chan struct{}
	done           chan struct{}
	err            error
	quotes         []contracts.Quote
	healthy        int
	callbackErr    error
	healthGate     chan struct{}
	healthEntered  chan struct{}
	healthExited   bool
	source         []byte
	closeTransport func()
	previousDialer *websocket.Dialer
}

func startHeartbeatStream(t *testing.T, fixture protocolFixture) *heartbeatStreamObservation {
	t.Helper()

	source, err := os.ReadFile(filepath.Join("testdata", "decoder", strings.ToLower(fixture.exchange)+".json"))
	if err != nil {
		t.Fatal(err)
	}

	dialer, accepted, closeTransport := heartbeatTransport(t)
	ctx, cancel := context.WithCancel(context.Background())
	observation := &heartbeatStreamObservation{
		ctx:            ctx,
		cancel:         cancel,
		done:           make(chan struct{}),
		source:         source,
		closeTransport: closeTransport,
		previousDialer: websocket.DefaultDialer,
	}
	websocket.DefaultDialer = dialer
	adapter := fixture.adapter("ws://heartbeat.test/stream")
	coin := "BTC"
	if fixture.exchange == "Binance" {
		coin = "BNB"
	}

	go func() {
		defer close(observation.done)

		observation.err = adapter.Stream(ctx, []string{coin}, func(quote contracts.Quote) error {
			observation.quotes = append(observation.quotes, quote)

			return observation.callbackErr
		}, func() {
			if observation.healthGate != nil {
				close(observation.healthEntered)
				<-observation.healthGate
				observation.healthExited = true
			}

			observation.healthy++
		})
	}()

	select {
	case server := <-accepted:
		if server.err != nil {
			observation.close()
			t.Fatal(server.err)
		}

		observation.peer = server.conn
	case <-observation.done:
		observation.close()
		t.Fatalf("stream failed during WebSocket upgrade: %v", observation.err)
	}

	observation.packets, observation.readerDone = observeHeartbeatPackets(observation.peer)
	synctest.Wait()
	subscription := takeHeartbeatPacket(t, observation.packets)
	if subscription.kind != websocket.TextMessage {
		observation.close()
		t.Fatalf("first packet was not a subscription: %#v", subscription)
	}

	return observation
}

func (o *heartbeatStreamObservation) close() {
	o.cancel()
	o.closeTransport()
	<-o.done
	if o.readerDone != nil {
		<-o.readerDone
	}

	websocket.DefaultDialer = o.previousDialer
}

func (o *heartbeatStreamObservation) send(t *testing.T, kind int, payload string) {
	t.Helper()

	if err := o.peer.WriteMessage(kind, []byte(payload)); err != nil {
		t.Fatalf("server could not send payload: %v; stream error: %v", err, o.err)
	}

	synctest.Wait()
}

func (o *heartbeatStreamObservation) reply(fixture protocolFixture) error {
	if fixture.exchange == "Binance" {
		return o.peer.WriteControl(websocket.PongMessage, []byte("heartbeat"), time.Now().Add(time.Second))
	}

	return o.peer.WriteMessage(websocket.TextMessage, []byte(fixture.heartbeatReply))
}

func assertStreamHeartbeatPacket(t *testing.T, fixture protocolFixture, packet heartbeatPacket) {
	t.Helper()

	if fixture.exchange == "Binance" {
		if packet.kind != websocket.PingMessage || packet.data != "heartbeat" {
			t.Fatalf("Binance heartbeat = %#v, want native ping with correlation payload", packet)
		}

		return
	}

	if packet.kind != websocket.TextMessage {
		t.Fatalf("%s heartbeat is not a text packet: %#v", fixture.exchange, packet)
	}

	if fixture.exchange == "Bybit" {
		var fields map[string]any

		if err := json.Unmarshal([]byte(packet.data), &fields); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(fields, map[string]any{
			"op":     "ping",
			"req_id": "heartbeat",
		}) {
			t.Fatalf("wrong Bybit heartbeat: %s", packet.data)
		}

		return
	}

	if packet.data != "ping" {
		t.Fatalf("%s heartbeat = %q, want plain ping", fixture.exchange, packet.data)
	}
}

func assertHeartbeatStreamReturned(t *testing.T, observation *heartbeatStreamObservation) {
	t.Helper()

	select {
	case <-observation.done:
	default:
		t.Fatal("Stream did not return after its terminal event")
	}

	select {
	case <-observation.readerDone:
	default:
		t.Fatal("Stream returned without closing its WebSocket")
	}
}

func assertNoHeartbeatAfterStream(t *testing.T, observation *heartbeatStreamObservation) {
	t.Helper()

	assertHeartbeatStreamReturned(t, observation)
	healthy := observation.healthy
	quotes := append([]contracts.Quote(nil), observation.quotes...)
	time.Sleep(time.Minute)
	synctest.Wait()

	if observation.healthy != healthy || !reflect.DeepEqual(observation.quotes, quotes) {
		t.Fatal("price or health callback occurred after Stream returned")
	}

	select {
	case packet := <-observation.packets:
		t.Fatalf("heartbeat sent after Stream returned: %#v", packet)
	default:
	}
}

func heartbeatMarketPayload(t *testing.T, fixture protocolFixture, source []byte, invalidate bool) []byte {
	t.Helper()

	return mutateDecoderMessage(t, source, func(fields map[string]any) {
		if fixture.exchange == "Binance" {
			fields["b"] = json.Number("7")

			return
		}

		var book map[string]any
		if fixture.exchange == "Bybit" {
			book = fields["data"].(map[string]any)
		} else {
			book = fields["data"].([]any)[0].(map[string]any)
		}

		key := "b"
		if fixture.exchange == "OKX" {
			key = "bids"
		}

		if invalidate {
			book[key] = []any{}
		} else {
			book[key].([]any)[0].([]any)[0] = json.Number("7")
		}
	})
}

func TestStreamsHeartbeatProtocolAndPriceIndependence(t *testing.T) {
	for _, fixture := range protocolFixtures() {
		t.Run(fixture.exchange, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				startedAt := time.Now()
				observation := startHeartbeatStream(t, fixture)
				defer observation.close()
				first := takeHeartbeatPacket(t, observation.packets)
				assertStreamHeartbeatPacket(t, fixture, first)
				if !first.at.Equal(startedAt) || len(observation.quotes) != 0 || observation.healthy != 0 {
					t.Fatal("initial heartbeat was delayed or changed price/health without a reply")
				}

				if err := observation.peer.WriteControl(websocket.PongMessage, []byte("unrelated"), time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				observation.send(t, websocket.TextMessage, fixture.controls[0])
				if fixture.exchange == "Bybit" {
					for _, payload := range []string{
						`{"op":"ping","success":true,"req_id":"other","ret_msg":"pong"}`,
						`{"op":"pong","success":true,"req_id":"heartbeat","ret_msg":"pong"}`,
						`{"op":"ping","success":true,"req_id":"heartbeat","ret_msg":"ok"}`,
						`{"op":"ping","req_id":"heartbeat","ret_msg":"pong"}`,
					} {
						observation.send(t, websocket.TextMessage, payload)
					}
				} else if fixture.exchange == "Binance" {
					observation.send(t, websocket.TextMessage, "pong")
				} else {
					if err := observation.peer.WriteControl(websocket.PongMessage, []byte("heartbeat"), time.Now().Add(time.Second)); err != nil {
						t.Fatal(err)
					}
					synctest.Wait()
					observation.send(t, websocket.TextMessage, "pong ")
					observation.send(t, websocket.TextMessage, `"pong"`)
				}

				if observation.healthy != 0 || len(observation.quotes) != 0 {
					t.Fatal("unmatched heartbeat or ACK changed health or emitted a price")
				}

				observation.send(t, websocket.TextMessage, string(observation.source))
				if observation.healthy != 0 || len(observation.quotes) != 1 {
					t.Fatal("market data confirmed health or failed to reach the price callback")
				}

				if fixture.exchange != "Binance" {
					observation.send(t, websocket.TextMessage, string(heartbeatMarketPayload(t, fixture, observation.source, true)))
					if len(observation.quotes) != 2 || !observation.quotes[1].Invalidated || observation.healthy != 0 {
						t.Fatal("invalidation confirmed health or lost its price callback")
					}
				}

				before := append([]contracts.Quote(nil), observation.quotes...)
				observation.send(t, websocket.TextMessage, "{")
				observation.send(t, websocket.TextMessage, string(heartbeatMarketPayload(t, fixture, observation.source, false)))
				if observation.healthy != 0 || !reflect.DeepEqual(observation.quotes, before) {
					t.Fatal("malformed market data changed health or emitted a price")
				}

				if err := observation.reply(fixture); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if observation.healthy != 1 || !reflect.DeepEqual(observation.quotes, before) {
					t.Fatal("valid heartbeat failed or changed quote fields/timestamps")
				}

				time.Sleep(20 * time.Second)
				synctest.Wait()
				second := takeHeartbeatPacket(t, observation.packets)
				assertStreamHeartbeatPacket(t, fixture, second)
				if second.at.Sub(startedAt) != 20*time.Second || observation.healthy != 1 {
					t.Fatal("quote silence or source timestamps changed the heartbeat schedule")
				}

				if err := observation.reply(fixture); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if observation.healthy != 2 || !reflect.DeepEqual(observation.quotes, before) {
					t.Fatal("second heartbeat failed or refreshed cached quote timestamps")
				}

				observation.cancel()
				synctest.Wait()
				assertNoHeartbeatAfterStream(t, observation)
			})
		})
	}
}

func TestStreamsTimeoutDespiteMarketTraffic(t *testing.T) {
	for _, fixture := range protocolFixtures() {
		t.Run(fixture.exchange, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				startedAt := time.Now()
				observation := startHeartbeatStream(t, fixture)
				defer observation.close()
				assertStreamHeartbeatPacket(t, fixture, takeHeartbeatPacket(t, observation.packets))

				for _, payload := range []string{string(observation.source), "{", string(heartbeatMarketPayload(t, fixture, observation.source, true)), fixture.controls[0]} {
					observation.send(t, websocket.TextMessage, payload)
					time.Sleep(time.Second)
					synctest.Wait()
				}

				time.Sleep(time.Second - time.Nanosecond)
				synctest.Wait()
				select {
				case <-observation.done:
					t.Fatal("Stream failed before the five-second heartbeat deadline")
				default:
				}

				time.Sleep(time.Nanosecond)
				synctest.Wait()
				assertHeartbeatStreamReturned(t, observation)
				var closeErr *websocket.CloseError
				var networkErr net.Error
				if observation.err == nil || errors.Is(observation.err, io.ErrClosedPipe) || errors.Is(observation.err, net.ErrClosed) ||
					errors.As(observation.err, &closeErr) || errors.As(observation.err, &networkErr) || observation.ctx.Err() != nil ||
					observation.healthy != 0 || time.Since(startedAt) != 5*time.Second {
					t.Fatalf("heartbeat timeout was masked by transport, market data or context: %v after %s", observation.err, time.Since(startedAt))
				}

				assertNoHeartbeatAfterStream(t, observation)
			})
		})
	}
}

func TestStreamsTerminalEventsStopHeartbeat(t *testing.T) {
	for _, fixture := range protocolFixtures() {
		t.Run(fixture.exchange, func(t *testing.T) {
			for _, terminal := range []string{"cancel", "transport", "request_rejected", "callback_error", "cancel_with_reply"} {
				t.Run(terminal, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						observation := startHeartbeatStream(t, fixture)
						defer observation.close()
						assertStreamHeartbeatPacket(t, fixture, takeHeartbeatPacket(t, observation.packets))
						callbackErr := errors.New("price callback stopped the stream")

						switch terminal {
						case "cancel":
							observation.cancel()
						case "transport":
							observation.peer.Close()
						case "request_rejected":
							observation.send(t, websocket.TextMessage, fixture.rejection)
						case "callback_error":
							observation.callbackErr = callbackErr
							observation.send(t, websocket.TextMessage, string(observation.source))
						case "cancel_with_reply":
							observation.cancel()
							observation.reply(fixture)
						}

						synctest.Wait()
						assertHeartbeatStreamReturned(t, observation)
						if observation.healthy != 0 {
							t.Fatal("terminal event or response after cancellation confirmed health")
						}

						if terminal == "request_rejected" && !errors.Is(observation.err, errRequestRejected) {
							t.Fatalf("request rejection lost its cause: %v", observation.err)
						}

						if terminal == "callback_error" && !errors.Is(observation.err, callbackErr) {
							t.Fatalf("callback failure lost its cause: %v", observation.err)
						}

						if terminal == "transport" && observation.err == nil {
							t.Fatal("transport closure returned no error")
						}

						assertNoHeartbeatAfterStream(t, observation)
					})
				})
			}
		})
	}
}

func TestStreamsWaitForHealthyCallbackBeforeReturning(t *testing.T) {
	for _, fixture := range protocolFixtures() {
		t.Run(fixture.exchange, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				observation := startHeartbeatStream(t, fixture)
				defer observation.close()
				assertStreamHeartbeatPacket(t, fixture, takeHeartbeatPacket(t, observation.packets))
				gate := make(chan struct{})
				defer func() {
					if gate != nil {
						close(gate)
					}
				}()
				observation.healthGate = gate
				observation.healthEntered = make(chan struct{})

				if err := observation.reply(fixture); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				select {
				case <-observation.healthEntered:
				default:
					t.Fatal("healthy callback was not entered")
				}

				observation.peer.Close()
				synctest.Wait()
				select {
				case <-observation.done:
					t.Fatal("Stream returned while its healthy callback was still running")
				default:
				}

				close(gate)
				gate = nil
				synctest.Wait()
				assertHeartbeatStreamReturned(t, observation)
				if !observation.healthExited || observation.healthy != 1 {
					t.Fatal("Stream did not join the healthy callback")
				}

				assertNoHeartbeatAfterStream(t, observation)
			})
		})
	}
}
