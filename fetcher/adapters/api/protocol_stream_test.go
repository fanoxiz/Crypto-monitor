package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/fetcher/core"
	"github.com/gorilla/websocket"
)

type protocolFixture struct {
	exchange       string
	adapter        func(string) core.Exchange
	subscription   string
	quote          string
	controls       []string
	rejection      string
	heartbeatReply string
}

func protocolFixtures() []protocolFixture {
	return []protocolFixture{
		{
			exchange: "Binance",
			adapter: func(endpoint string) core.Exchange {
				return BinanceAdapter{
					URL: endpoint,
				}
			},
			subscription: `{"method":"SUBSCRIBE","params":["ethusdt@bookTicker","xautusdt@bookTicker"],"id":1}`,
			quote:        `{"s":"%sUSDT","b":"99.125","B":"1.75","a":"100.25","A":"2.625","E":1700000000999}`,
			controls: []string{
				`{"result":null,"id":1}`,
				`{"result":null,"id":1}`,
				`{"result":null,"id":2}`,
				`{"result":null,"id":"1"}`,
				`{"event":"notice","msg":"connected"}`,
				`{"code":"bad-type"}`,
				`{"s":"ETHUSDT","b":99,"B":"1","a":"100","A":"2"}`,
				`{"b":"99","B":"1","a":"100","A":"2"}`,
			},
			rejection: `{"code":2,"msg":"invalid request","id":1}`,
		},
		{
			exchange: "Bybit",
			adapter: func(endpoint string) core.Exchange {
				return BybitAdapter{
					URL: endpoint,
				}
			},
			subscription: `{"op":"subscribe","args":["orderbook.1.ETHUSDT","orderbook.1.XAUTUSDT"],"req_id":"quotes"}`,
			quote:        `{"cts":1700000000123,"ts":1700000000456,"data":{"s":"%sUSDT","b":[["99.125","1.75"]],"a":[["100.25","2.625"]]}}`,
			controls: []string{
				`{"op":"subscribe","success":true,"req_id":"quotes"}`,
				`{"op":"subscribe","success":true,"req_id":"quotes"}`,
				`{"op":"subscribe","success":true,"req_id":"other"}`,
				`{"op":"ping","success":true,"req_id":"other","ret_msg":"pong"}`,
				`{"op":"auth","success":true,"req_id":"other"}`,
				`{"op":"subscribe","success":"bad-type","req_id":"quotes"}`,
				`{"data":{"s":"ETHUSDT","b":[[99,"1"]],"a":[["100","2"]]}}`,
				`{"data":{"s":"ETHUSDT"}}`,
			},
			rejection:      `{"op":"subscribe","success":false,"req_id":"quotes","ret_msg":"invalid topic"}`,
			heartbeatReply: `{"op":"ping","success":true,"req_id":"heartbeat","ret_msg":"pong"}`,
		},
		{
			exchange: "Bitget",
			adapter: func(endpoint string) core.Exchange {
				return BitgetAdapter{
					URL: endpoint,
				}
			},
			subscription: `{"op":"subscribe","args":[{"instType":"spot","topic":"books1","symbol":"ETHUSDT"},{"instType":"spot","topic":"books1","symbol":"XAUTUSDT"}]}`,
			quote:        `{"ts":1700000000456,"arg":{"symbol":"%sUSDT"},"data":[{"ts":"1700000000123","b":[["99.125","1.75"]],"a":[["100.25","2.625"]]},{"ts":"1700000000789","b":[["999","9"]],"a":[["1000","10"]]}]}`,
			controls: []string{
				`{"event":"subscribe","arg":{"instType":"spot","topic":"books1","symbol":"ETHUSDT"}}`,
				`{"event":"subscribe","arg":{"instType":"spot","topic":"books1","symbol":"ETHUSDT"}}`,
				`{"event":"subscribe","arg":{"instType":"spot","topic":"books1","symbol":"BTCUSDT"}}`,
				`{"event":"subscribe","arg":{"instType":"spot","topic":"books5","symbol":"ETHUSDT"}}`,
				`{"event":"notice","msg":"connected"}`,
				`{"event":"subscribe","arg":{"topic":7}}`,
				`{"arg":{"symbol":"ETHUSDT"},"data":[{"b":[[99,"1"]],"a":[["100","2"]]}]}`,
				`{"arg":{"symbol":"ETHUSDT"},"data":[{}]}`,
			},
			rejection:      `{"event":"error","code":"30001","msg":"invalid topic","arg":{"symbol":"ETHUSDT"}}`,
			heartbeatReply: "pong",
		},
		{
			exchange: "OKX",
			adapter: func(endpoint string) core.Exchange {
				return OKXAdapter{
					URL: endpoint,
				}
			},
			subscription: `{"op":"subscribe","args":[{"channel":"bbo-tbt","instId":"ETH-USDT"},{"channel":"bbo-tbt","instId":"XAUT-USDT"}]}`,
			quote:        `{"ts":1700000000456,"arg":{"instId":"%s-USDT"},"data":[{"ts":"1700000000123","bids":[["99.125","1.75"]],"asks":[["100.25","2.625"]]},{"ts":"1700000000789","bids":[["999","9"]],"asks":[["1000","10"]]}]}`,
			controls: []string{
				`{"event":"subscribe","arg":{"channel":"bbo-tbt","instId":"ETH-USDT"}}`,
				`{"event":"subscribe","arg":{"channel":"bbo-tbt","instId":"ETH-USDT"}}`,
				`{"event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDT"}}`,
				`{"event":"subscribe","arg":{"channel":"books","instId":"ETH-USDT"}}`,
				`{"event":"notice","msg":"connected"}`,
				`{"event":"subscribe","arg":{"channel":7}}`,
				`{"arg":{"instId":"ETH-USDT"},"data":[{"bids":[[99,"1"]],"asks":[["100","2"]]}]}`,
				`{"arg":{"instId":"ETH-USDT"},"data":[{}]}`,
			},
			rejection:      `{"event":"error","code":"60018","msg":"invalid channel","arg":{"instId":"ETH-USDT"}}`,
			heartbeatReply: "pong",
		},
	}
}

func runProtocolStream(t *testing.T, fixture protocolFixture, reject bool) (any, []contracts.Quote, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	requests := make(chan []byte, 1)
	serverDone := make(chan error, 1)
	readerErrors := make(chan error, 1)
	firstQuote := make(chan struct{})
	healthy := make(chan struct{}, 1)
	streamDone := make(chan struct{})
	finished := errors.New("protocol scenario complete")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverDone <- func() error {
			upgrader := websocket.Upgrader{}
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return err
			}
			defer conn.Close()
			stop := context.AfterFunc(ctx, func() {
				conn.Close()
			})
			defer stop()

			_, request, err := conn.ReadMessage()
			if err != nil {
				return err
			}
			requests <- request

			var writer sync.Mutex
			write := func(payload string) error {
				writer.Lock()
				defer writer.Unlock()

				if err := conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
					return err
				}

				return conn.WriteMessage(websocket.TextMessage, []byte(payload))
			}
			conn.SetPingHandler(func(payload string) error {
				if fixture.exchange != "Binance" || payload != "heartbeat" {
					err := fmt.Errorf("unexpected native heartbeat from %s: %q", fixture.exchange, payload)
					readerErrors <- err

					return err
				}

				return conn.WriteControl(websocket.PongMessage, []byte(payload), time.Now().Add(3*time.Second))
			})
			readerDone := make(chan struct{})

			go func() {
				defer close(readerDone)
				defer conn.Close()

				for {
					_, packet, err := conn.ReadMessage()
					if err != nil {
						return
					}

					valid := string(packet) == "ping" && (fixture.exchange == "Bitget" || fixture.exchange == "OKX")
					if fixture.exchange == "Bybit" {
						var heartbeat map[string]any

						valid = json.Unmarshal(packet, &heartbeat) == nil && reflect.DeepEqual(heartbeat, map[string]any{
							"op":     "ping",
							"req_id": "heartbeat",
						})
					}

					if !valid {
						readerErrors <- fmt.Errorf("unexpected heartbeat from %s: %s", fixture.exchange, packet)

						return
					}

					if err := write(fixture.heartbeatReply); err != nil {
						return
					}
				}
			}()
			defer func() {
				conn.Close()
				<-readerDone
			}()

			if reject {
				if err := write(fixture.rejection); err != nil {
					return err
				}
			} else {
				if err := write(fmt.Sprintf(fixture.quote, "XAUT")); err != nil {
					return err
				}

				select {
				case <-firstQuote:
				case <-streamDone:
					return nil
				case <-ctx.Done():
					return nil
				}

				for _, payload := range append(fixture.controls, "{", "{}", "null") {
					if err := write(payload); err != nil {
						return err
					}
				}

				select {
				case <-healthy:
				case <-streamDone:
					return nil
				case <-ctx.Done():
					return nil
				}

				if err := write(fmt.Sprintf(fixture.quote, "ETH")); err != nil {
					return err
				}
			}

			select {
			case <-streamDone:
			case <-ctx.Done():
			}

			return nil
		}()
	}))
	t.Cleanup(server.Close)
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	adapter := fixture.adapter(endpoint)
	if adapter.Name() != fixture.exchange {
		t.Fatalf("adapter name = %s, want %s", adapter.Name(), fixture.exchange)
	}
	var quotes []contracts.Quote

	err := adapter.Stream(ctx, []string{"ETH", "XAUT"}, func(quote contracts.Quote) error {
		quotes = append(quotes, quote)
		if len(quotes) == 1 {
			close(firstQuote)
		}

		if len(quotes) == 2 {
			return finished
		}

		return nil
	}, func() {
		select {
		case healthy <- struct{}{}:
		default:
		}
	})
	close(streamDone)
	cancel()

	select {
	case serverErr := <-serverDone:
		if serverErr != nil {
			t.Fatalf("WebSocket fixture: %v; stream error: %v; quotes: %#v", serverErr, err, quotes)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("WebSocket fixture did not stop")
	}

	select {
	case readerErr := <-readerErrors:
		t.Fatal(readerErr)
	default:
	}

	var request any

	select {
	case payload := <-requests:
		if err := json.Unmarshal(payload, &request); err != nil {
			t.Fatalf("invalid subscription JSON: %v", err)
		}
	default:
		t.Fatalf("adapter sent no subscription: %v", err)
	}

	if !reject && !errors.Is(err, finished) {
		t.Fatalf("stream stopped before processing both quotes: %v; quotes: %#v", err, quotes)
	}

	return request, quotes, err
}

func TestAdaptersSubscribeAndContinueAfterProtocolNoise(t *testing.T) {
	for _, fixture := range protocolFixtures() {
		t.Run(fixture.exchange, func(t *testing.T) {
			startedAt := time.Now().UTC()
			request, quotes, _ := runProtocolStream(t, fixture, false)
			finishedAt := time.Now().UTC()
			var wantRequest any

			if err := json.Unmarshal([]byte(fixture.subscription), &wantRequest); err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(request, wantRequest) {
				t.Fatalf("subscription = %#v, want %#v", request, wantRequest)
			}

			if len(quotes) != 2 {
				t.Fatalf("quotes = %#v, want only XAUT before ACK and ETH after protocol noise", quotes)
			}

			for i, coin := range []string{"XAUT", "ETH"} {
				quote := quotes[i]
				want := contracts.Quote{
					Coin:       coin,
					Exchange:   fixture.exchange,
					BidPrice:   "99.125",
					BidSize:    "1.75",
					AskPrice:   "100.25",
					AskSize:    "2.625",
					ReceivedAt: quote.ReceivedAt,
				}
				if fixture.exchange != "Binance" {
					want.SourceAt = time.Date(2023, 11, 14, 22, 13, 20, 123000000, time.UTC)
				}

				if !reflect.DeepEqual(quote, want) || quote.ReceivedAt.Before(startedAt) || quote.ReceivedAt.After(finishedAt) {
					t.Fatalf("wrong quote fields or reception time: %#v, want %#v", quote, want)
				}
			}
		})
	}
}

func TestAdaptersStopOnExchangeRequestRejection(t *testing.T) {
	for _, fixture := range protocolFixtures() {
		t.Run(fixture.exchange, func(t *testing.T) {
			_, quotes, err := runProtocolStream(t, fixture, true)
			if !errors.Is(err, errRequestRejected) {
				t.Fatalf("stream error = %v, want exchange request rejection", err)
			}

			if len(quotes) != 0 {
				t.Fatalf("rejected request produced quotes: %#v", quotes)
			}
		})
	}
}
