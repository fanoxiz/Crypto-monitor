package api

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/gorilla/websocket"
)

type subscriptionStreamObservation struct {
	mutex   sync.Mutex
	quotes  []contracts.Quote
	healthy int
}

func (o *subscriptionStreamObservation) snapshot() ([]contracts.Quote, int) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	return append([]contracts.Quote(nil), o.quotes...), o.healthy
}

func assertPartialSubscriptionReports(t *testing.T, logs *reconnectLogBuffer, coin string, reports int, acknowledged bool, confirmsAll bool, at time.Time) {
	t.Helper()

	var summaries []subscriptionLogRecord
	latest := make(map[string]subscriptionLogRecord)
	for _, record := range subscriptionLogRecords(t, logs) {
		if record.Message == "instrument_status" && record.Reason == "stream_summary" {
			summaries = append(summaries, record)
			latest[record.Coin] = record
		}
	}

	if len(summaries) != reports*2 || len(latest) != 2 {
		t.Fatalf("incomplete coverage was not reported for both instruments: %#v", summaries)
	}
	active, activePresent := latest[coin]
	silent, silentPresent := latest["XAUT"]
	activeLevel := "WARN"
	if acknowledged {
		activeLevel = "INFO"
	}

	if !activePresent || active.Confirmed != acknowledged || !active.DataReceived || !active.QuoteReceived || active.Invalidated ||
		active.Level != activeLevel || !active.LastDataAt.Equal(at) || !active.LastQuoteAt.Equal(at) ||
		active.DataAge == nil || *active.DataAge != 0 || active.QuoteAge == nil || *active.QuoteAge != 0 {
		t.Fatalf("incorrect active instrument coverage: %#v", active)
	}
	if !silentPresent || silent.Confirmed != (acknowledged && confirmsAll) || silent.DataReceived || silent.QuoteReceived || silent.Invalidated ||
		silent.Level != "WARN" || !silent.LastDataAt.IsZero() || !silent.LastQuoteAt.IsZero() || silent.DataAge != nil || silent.QuoteAge != nil {
		t.Fatalf("silent instrument inherited another instrument's state: %#v", silent)
	}
}

func TestStreamsKeepSilentInstrumentsDiagnostic(t *testing.T) {
	for _, fixture := range protocolFixtures() {
		t.Run(fixture.exchange, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				source, err := os.ReadFile(filepath.Join("testdata", "decoder", strings.ToLower(fixture.exchange)+".json"))
				if err != nil {
					t.Fatal(err)
				}

				coin := "BTC"
				if fixture.exchange == "Binance" {
					coin = "BNB"
				}
				var logs reconnectLogBuffer
				previousLogger := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
				previousDialer := websocket.DefaultDialer
				dialer, accepted, closeTransport := heartbeatTransport(t)
				websocket.DefaultDialer = dialer
				ctx, cancel := context.WithCancel(context.Background())
				adapter := fixture.adapter("ws://heartbeat.test/subscriptions")
				observation := &subscriptionStreamObservation{}
				done := make(chan struct{})
				var streamErr error
				var readerDone <-chan struct{}

				go func() {
					defer close(done)

					streamErr = adapter.Stream(ctx, []string{coin, "XAUT"}, func(quote contracts.Quote) error {
						observation.mutex.Lock()
						observation.quotes = append(observation.quotes, quote)
						observation.mutex.Unlock()

						return nil
					}, func() {
						observation.mutex.Lock()
						observation.healthy++
						observation.mutex.Unlock()
					})
				}()

				defer func() {
					cancel()
					closeTransport()
					<-done
					if readerDone != nil {
						<-readerDone
					}

					websocket.DefaultDialer = previousDialer
					slog.SetDefault(previousLogger)
				}()

				server := <-accepted
				if server.err != nil {
					t.Fatal(server.err)
				}
				packets, reader := observeHeartbeatPackets(server.conn)
				readerDone = reader
				synctest.Wait()
				expectedSubscription := strings.ReplaceAll(fixture.subscription, "ETH", coin)
				expectedSubscription = strings.ReplaceAll(expectedSubscription, "ethusdt@", strings.ToLower(coin)+"usdt@")
				assertReconnectSubscription(t, packets, expectedSubscription)
				assertStreamHeartbeatPacket(t, fixture, takeHeartbeatPacket(t, packets))
				sendReconnectPayload(t, server.conn, source)

				reply := func() {
					t.Helper()

					var err error
					if fixture.exchange == "Binance" {
						err = server.conn.WriteControl(websocket.PongMessage, []byte("heartbeat"), time.Now().Add(time.Second))
					} else {
						err = server.conn.WriteMessage(websocket.TextMessage, []byte(fixture.heartbeatReply))
					}
					if err != nil {
						t.Fatal(err)
					}

					synctest.Wait()
				}

				reply()
				for step := 1; step <= 4; step++ {
					time.Sleep(20 * time.Second)
					synctest.Wait()
					assertStreamHeartbeatPacket(t, fixture, takeHeartbeatPacket(t, packets))
					sendReconnectPayload(t, server.conn, source)
					reply()

					select {
					case <-done:
						t.Fatalf("missing ACK or silent instrument terminated a healthy stream: %v", streamErr)
					default:
					}

					quotes, healthy := observation.snapshot()
					if len(quotes) != step+1 || healthy != step+1 {
						t.Fatalf("price or heartbeat processing stopped: quotes=%d, healthy=%d, want %d each", len(quotes), healthy, step+1)
					}
					for _, quote := range quotes {
						if quote.Coin != coin || quote.Exchange != fixture.exchange || quote.Invalidated {
							t.Fatalf("silent instrument or ACK emitted a quote: %#v", quote)
						}
					}

					if step == 2 || step == 4 {
						confirmsAll := fixture.exchange == "Binance" || fixture.exchange == "Bybit"
						assertPartialSubscriptionReports(t, &logs, coin, step/2, step == 4, confirmsAll, time.Now().UTC())
					}
					if step == 2 {
						ack := strings.ReplaceAll(fixture.controls[0], "ETH", coin)
						sendReconnectPayload(t, server.conn, []byte(ack))
						afterQuotes, afterHealthy := observation.snapshot()
						if len(afterQuotes) != len(quotes) || afterHealthy != healthy {
							t.Fatal("subscription ACK changed prices or confirmed heartbeat")
						}
					}
				}

				cancel()
				synctest.Wait()
				select {
				case <-done:
				default:
					t.Fatal("stream did not stop on cancellation")
				}
			})
		})
	}
}

func TestStreamsReportMissingDataWithoutMarketMessages(t *testing.T) {
	for _, fixture := range protocolFixtures() {
		t.Run(fixture.exchange, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var logs reconnectLogBuffer
				previousLogger := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
				defer slog.SetDefault(previousLogger)
				observation := startHeartbeatStream(t, fixture)
				defer observation.close()
				coin := "BTC"
				if fixture.exchange == "Binance" {
					coin = "BNB"
				}

				assertStreamHeartbeatPacket(t, fixture, takeHeartbeatPacket(t, observation.packets))
				ack := strings.ReplaceAll(fixture.controls[0], "ETH", coin)
				observation.send(t, websocket.TextMessage, ack)
				if err := observation.reply(fixture); err != nil {
					t.Fatal(err)
				}

				synctest.Wait()
				for step := 1; step <= 4; step++ {
					time.Sleep(20 * time.Second)
					synctest.Wait()
					assertStreamHeartbeatPacket(t, fixture, takeHeartbeatPacket(t, observation.packets))
					if err := observation.reply(fixture); err != nil {
						t.Fatal(err)
					}

					synctest.Wait()
					select {
					case <-observation.done:
						t.Fatalf("market silence terminated a healthy connection: %v", observation.err)
					default:
					}

					if len(observation.quotes) != 0 || observation.healthy != step+1 {
						t.Fatalf("heartbeat without market data: quotes=%d, healthy=%d, want zero quotes and %d confirmations", len(observation.quotes), observation.healthy, step+1)
					}

					var reports []subscriptionLogRecord
					for _, record := range subscriptionLogRecords(t, &logs) {
						if record.Message == "instrument_status" && record.Reason == "stream_summary" {
							reports = append(reports, record)
						}
					}

					if len(reports) != step/2 {
						t.Fatalf("heartbeat did not emit periodic missing-data diagnostics: got %d reports, want %d", len(reports), step/2)
					}
					for _, record := range reports {
						if record.Coin != coin || record.Level != "WARN" || !record.Confirmed || record.DataReceived || record.QuoteReceived || record.Invalidated ||
							!record.LastDataAt.IsZero() || !record.LastQuoteAt.IsZero() || record.DataAge != nil || record.QuoteAge != nil {
							t.Fatalf("incorrect warning for a subscribed instrument without data: %#v", record)
						}
					}
				}

				observation.cancel()
				synctest.Wait()
				assertNoHeartbeatAfterStream(t, observation)
			})
		})
	}
}
