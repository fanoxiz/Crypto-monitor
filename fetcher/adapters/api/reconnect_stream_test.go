package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/fetcher/core"
	"github.com/gorilla/websocket"
)

type reconnectStreamStore struct {
	quotes []contracts.Quote
}

type reconnectLogBuffer struct {
	mutex sync.Mutex
	data  bytes.Buffer
}

func (b *reconnectLogBuffer) Write(data []byte) (int, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.data.Write(data)
}

func (b *reconnectLogBuffer) snapshot() []byte {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return append([]byte(nil), b.data.Bytes()...)
}

func (s *reconnectStreamStore) SaveBatch(_ context.Context, quotes []contracts.Quote) error {
	s.quotes = append(s.quotes, quotes...)

	return nil
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

func TestServiceReconnectsAdaptersWithFreshStreamState(t *testing.T) {
	subscriptions := map[string]string{
		"Binance": `{"method":"SUBSCRIBE","params":["bnbusdt@bookTicker"],"id":1}`,
		"Bybit":   `{"op":"subscribe","args":["orderbook.1.BTCUSDT"],"req_id":"quotes"}`,
		"Bitget":  `{"op":"subscribe","args":[{"instType":"spot","topic":"books1","symbol":"BTCUSDT"}]}`,
		"OKX":     `{"op":"subscribe","args":[{"channel":"bbo-tbt","instId":"BTC-USDT"}]}`,
	}

	for _, fixture := range protocolFixtures() {
		t.Run(fixture.exchange, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source, err := os.ReadFile(filepath.Join("testdata", "decoder", strings.ToLower(fixture.exchange)+".json"))
				if err != nil {
					t.Fatal(err)
				}

				var logs reconnectLogBuffer
				previousLogger := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
				previousDialer := websocket.DefaultDialer
				firstDialer, firstAccepted, closeFirst := heartbeatTransport(t)
				secondDialer, secondAccepted, closeSecond := heartbeatTransport(t)
				dialers := []*websocket.Dialer{firstDialer, secondDialer}
				var dialMutex sync.Mutex
				var dialedAt []time.Time
				dialSnapshot := func() []time.Time {
					dialMutex.Lock()
					defer dialMutex.Unlock()

					return append([]time.Time(nil), dialedAt...)
				}
				websocket.DefaultDialer = &websocket.Dialer{
					HandshakeTimeout: time.Second,
					NetDialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
						dialMutex.Lock()
						index := len(dialedAt)
						dialedAt = append(dialedAt, time.Now())
						dialMutex.Unlock()

						if index >= len(dialers) {
							return nil, errors.New("unexpected extra connection attempt")
						}

						return dialers[index].NetDialContext(ctx, network, address)
					},
				}
				ctx, cancel := context.WithCancel(context.Background())
				store := &reconnectStreamStore{}
				adapter := fixture.adapter("ws://heartbeat.test/reconnect")
				service := core.NewFetcherService([]core.Exchange{adapter}, store)
				coin := "BTC"
				if fixture.exchange == "Binance" {
					coin = "BNB"
				}
				done := make(chan struct{})
				var readers []<-chan struct{}

				go func() {
					defer close(done)

					service.Run(ctx, []string{coin})
				}()

				defer func() {
					cancel()
					closeFirst()
					closeSecond()
					<-done
					for _, reader := range readers {
						<-reader
					}

					websocket.DefaultDialer = previousDialer
					slog.SetDefault(previousLogger)
				}()

				first := <-firstAccepted
				if first.err != nil {
					t.Fatal(first.err)
				}

				firstPackets, firstReader := observeHeartbeatPackets(first.conn)
				readers = append(readers, firstReader)
				synctest.Wait()
				assertReconnectSubscription(t, firstPackets, subscriptions[fixture.exchange])
				assertStreamHeartbeatPacket(t, fixture, takeHeartbeatPacket(t, firstPackets))

				if fixture.exchange == "Binance" {
					err = first.conn.WriteControl(websocket.PongMessage, []byte("heartbeat"), time.Now().Add(time.Second))
				} else {
					err = first.conn.WriteMessage(websocket.TextMessage, []byte(fixture.heartbeatReply))
				}
				if err != nil {
					t.Fatal(err)
				}

				synctest.Wait()
				ack := strings.ReplaceAll(fixture.controls[0], "ETH", coin)
				sendReconnectPayload(t, first.conn, []byte(ack))
				sendReconnectPayload(t, first.conn, source)
				if len(store.quotes) != 1 || store.quotes[0].Coin != coin || store.quotes[0].Exchange != fixture.exchange {
					t.Fatalf("first connection did not deliver its quote: %#v", store.quotes)
				}

				first.conn.Close()
				synctest.Wait()
				time.Sleep(500*time.Millisecond - time.Nanosecond)
				synctest.Wait()
				if attempts := dialSnapshot(); len(attempts) != 1 {
					t.Fatalf("reconnected before the first backoff ended: %v", attempts)
				}

				time.Sleep(time.Nanosecond)
				second := <-secondAccepted
				if second.err != nil {
					t.Fatal(second.err)
				}

				secondPackets, secondReader := observeHeartbeatPackets(second.conn)
				readers = append(readers, secondReader)
				synctest.Wait()
				if attempts := dialSnapshot(); len(attempts) != 2 || attempts[1].Sub(attempts[0]) != 500*time.Millisecond {
					t.Fatalf("unexpected reconnect schedule: %v", attempts)
				}
				assertReconnectSubscription(t, secondPackets, subscriptions[fixture.exchange])
				assertStreamHeartbeatPacket(t, fixture, takeHeartbeatPacket(t, secondPackets))
				if len(store.quotes) != 1 {
					t.Fatal("reconnecting or heartbeat replayed an old quote")
				}

				sendReconnectPayload(t, second.conn, source)
				expected := store.quotes[0]
				expected.ReceivedAt = time.Now().UTC()
				if len(store.quotes) != 2 || !reflect.DeepEqual(store.quotes[1], expected) ||
					store.quotes[1].ReceivedAt.Sub(store.quotes[0].ReceivedAt) != 500*time.Millisecond {
					t.Fatalf("new stream did not independently process a price before ACK: %#v", store.quotes)
				}

				decoder := json.NewDecoder(bytes.NewReader(logs.snapshot()))
				var confirmed []bool
				for {
					var record struct {
						Message   string `json:"msg"`
						Coin      string `json:"coin"`
						Confirmed bool   `json:"confirmed"`
					}
					err := decoder.Decode(&record)
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}

					if record.Message == "subscription_data_observed" && record.Coin == coin {
						confirmed = append(confirmed, record.Confirmed)
					}
				}

				if !reflect.DeepEqual(confirmed, []bool{true, false}) {
					t.Fatalf("new stream inherited subscription state: %v", confirmed)
				}

				time.Sleep(5*time.Second - time.Nanosecond)
				synctest.Wait()
				select {
				case <-secondReader:
					t.Fatal("new stream inherited the previous heartbeat deadline")
				default:
				}

				time.Sleep(time.Nanosecond)
				synctest.Wait()
				select {
				case <-secondReader:
				default:
					t.Fatal("previous healthy heartbeat confirmed the new connection")
				}

				if attempts := dialSnapshot(); len(attempts) != 2 || ctx.Err() != nil {
					t.Fatalf("heartbeat timeout bypassed retry or cancelled the service: attempts=%d, error=%v", len(attempts), ctx.Err())
				}

				cancel()
				synctest.Wait()
				select {
				case <-done:
				default:
					t.Fatal("service did not stop during reconnect backoff")
				}

				time.Sleep(time.Minute)
				synctest.Wait()
				if len(dialSnapshot()) != 2 || len(store.quotes) != 2 {
					t.Fatal("connection or quote callback outlived the service")
				}
			})
		})
	}
}
