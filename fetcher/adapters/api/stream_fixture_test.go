package api

import (
	"context"
	"encoding/json"
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

func assertReconnectSubscription(t *testing.T, packets <-chan heartbeatPacket, expected string) {
	t.Helper()

	packet := takeHeartbeatPacket(t, packets)
	if packet.kind != websocket.TextMessage {
		t.Fatalf("first packet after connecting was not a subscription: %#v", packet)
	}

	var actual, wanted any
	if err := json.Unmarshal([]byte(packet.data), &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(expected), &wanted); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(actual, wanted) {
		t.Fatalf("subscription = %s, want %s", packet.data, expected)
	}
}

func sendReconnectPayload(t *testing.T, peer *websocket.Conn, payload []byte) {
	t.Helper()

	if err := peer.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatal(err)
	}

	synctest.Wait()
}
