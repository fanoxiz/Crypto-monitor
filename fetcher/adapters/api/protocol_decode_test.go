package api

import (
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"
)

func checkProtocolMessage(t *testing.T, name string, decode func([]byte) (streamMessage, error), payload string, want streamMessage, wantErr error) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		message, err := decode([]byte(payload))
		if !errors.Is(err, wantErr) {
			t.Fatalf("decoder error = %v, want %v", err, wantErr)
		}

		if !reflect.DeepEqual(message, want) {
			t.Fatalf("message = %#v, want %#v", message, want)
		}

		if wantErr != nil {
			return
		}

		instruments := map[string]*instrumentStatus{
			"ETH":  {},
			"XAUT": {},
		}
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))

		for range 2 {
			recordMessage(logger, instruments, message, time.Now().UTC())
		}

		for coin, status := range instruments {
			confirmed := want.confirmedAll || want.confirmedCoin == coin
			if status.confirmed != confirmed || !status.lastDataAt.IsZero() || !status.lastQuoteAt.IsZero() || status.invalidated {
				t.Fatalf("repeated control message changed instrument %s unexpectedly: %#v", coin, status)
			}
		}

		if len(instruments) != 2 {
			t.Fatal("unexpected acknowledgment added an instrument")
		}
	})
}

func TestBinanceProtocolMessages(t *testing.T) {
	checkProtocolMessage(t, "ack", decodeBinance, `{"result":null,"id":1}`, streamMessage{
		confirmedAll: true,
	}, nil)
	checkProtocolMessage(t, "wrong_id", decodeBinance, `{"result":null,"id":2}`, streamMessage{}, nil)
	checkProtocolMessage(t, "string_id", decodeBinance, `{"result":null,"id":"1"}`, streamMessage{}, nil)
	checkProtocolMessage(t, "missing_id", decodeBinance, `{"result":null}`, streamMessage{}, nil)
	checkProtocolMessage(t, "non_null_result", decodeBinance, `{"result":{},"id":1}`, streamMessage{}, nil)
	checkProtocolMessage(t, "missing_result", decodeBinance, `{"id":1}`, streamMessage{}, nil)
	checkProtocolMessage(t, "service_message", decodeBinance, `{"event":"notice","msg":"connected"}`, streamMessage{}, nil)
	checkProtocolMessage(t, "rejected_request", decodeBinance, `{"code":2,"msg":"invalid request","id":1}`, streamMessage{}, errRequestRejected)
}

func TestBybitProtocolMessages(t *testing.T) {
	checkProtocolMessage(t, "ack", decodeBybit, `{"op":"subscribe","success":true,"req_id":"quotes"}`, streamMessage{
		confirmedAll: true,
	}, nil)
	checkProtocolMessage(t, "wrong_request_id", decodeBybit, `{"op":"subscribe","success":true,"req_id":"other"}`, streamMessage{}, nil)
	checkProtocolMessage(t, "missing_request_id", decodeBybit, `{"op":"subscribe","success":true}`, streamMessage{}, nil)
	checkProtocolMessage(t, "missing_success", decodeBybit, `{"op":"subscribe","req_id":"quotes"}`, streamMessage{}, nil)
	checkProtocolMessage(t, "heartbeat", decodeBybit, `{"op":"ping","success":true,"req_id":"heartbeat","ret_msg":"pong"}`, streamMessage{
		heartbeat: true,
	}, nil)
	checkProtocolMessage(t, "wrong_heartbeat_id", decodeBybit, `{"op":"ping","success":true,"req_id":"quotes","ret_msg":"pong"}`, streamMessage{}, nil)
	checkProtocolMessage(t, "wrong_heartbeat_operation", decodeBybit, `{"op":"pong","success":true,"req_id":"heartbeat","ret_msg":"pong"}`, streamMessage{}, nil)
	checkProtocolMessage(t, "wrong_heartbeat_message", decodeBybit, `{"op":"ping","success":true,"req_id":"heartbeat","ret_msg":"ok"}`, streamMessage{}, nil)
	checkProtocolMessage(t, "missing_heartbeat_success", decodeBybit, `{"op":"ping","req_id":"heartbeat","ret_msg":"pong"}`, streamMessage{}, nil)
	checkProtocolMessage(t, "service_message", decodeBybit, `{"op":"auth","success":true,"req_id":"other"}`, streamMessage{}, nil)
	checkProtocolMessage(t, "rejected_subscription", decodeBybit, `{"op":"subscribe","success":false,"req_id":"quotes","ret_msg":"invalid topic"}`, streamMessage{}, errRequestRejected)
	checkProtocolMessage(t, "rejected_heartbeat", decodeBybit, `{"op":"ping","success":false,"req_id":"heartbeat","ret_msg":"refused"}`, streamMessage{}, errRequestRejected)
}

func TestBitgetProtocolMessages(t *testing.T) {
	checkProtocolMessage(t, "ack", decodeBitget, `{"event":"subscribe","arg":{"instType":"spot","topic":"books1","symbol":"ETHUSDT"}}`, streamMessage{
		confirmedCoin: "ETH",
	}, nil)
	checkProtocolMessage(t, "unexpected_instrument", decodeBitget, `{"event":"subscribe","arg":{"instType":"spot","topic":"books1","symbol":"BTCUSDT"}}`, streamMessage{
		confirmedCoin: "BTC",
	}, nil)
	checkProtocolMessage(t, "wrong_channel", decodeBitget, `{"event":"subscribe","arg":{"instType":"spot","topic":"books5","symbol":"ETHUSDT"}}`, streamMessage{}, nil)
	checkProtocolMessage(t, "wrong_market", decodeBitget, `{"event":"subscribe","arg":{"instType":"futures","topic":"books1","symbol":"ETHUSDT"}}`, streamMessage{}, nil)
	checkProtocolMessage(t, "missing_instrument", decodeBitget, `{"event":"subscribe","arg":{"instType":"spot","topic":"books1"}}`, streamMessage{}, nil)
	checkProtocolMessage(t, "service_message", decodeBitget, `{"event":"notice","code":"0","msg":"connected"}`, streamMessage{}, nil)
	checkProtocolMessage(t, "rejected_request", decodeBitget, `{"event":"error","code":"30001","msg":"invalid topic","arg":{"symbol":"ETHUSDT"}}`, streamMessage{}, errRequestRejected)
}

func TestOKXProtocolMessages(t *testing.T) {
	checkProtocolMessage(t, "ack", decodeOKX, `{"event":"subscribe","arg":{"channel":"bbo-tbt","instId":"ETH-USDT"}}`, streamMessage{
		confirmedCoin: "ETH",
	}, nil)
	checkProtocolMessage(t, "unexpected_instrument", decodeOKX, `{"event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDT"}}`, streamMessage{
		confirmedCoin: "BTC",
	}, nil)
	checkProtocolMessage(t, "wrong_channel", decodeOKX, `{"event":"subscribe","arg":{"channel":"books","instId":"ETH-USDT"}}`, streamMessage{}, nil)
	checkProtocolMessage(t, "missing_instrument", decodeOKX, `{"event":"subscribe","arg":{"channel":"bbo-tbt"}}`, streamMessage{}, nil)
	checkProtocolMessage(t, "service_message", decodeOKX, `{"event":"notice","code":"0","msg":"connected"}`, streamMessage{}, nil)
	checkProtocolMessage(t, "rejected_request", decodeOKX, `{"event":"error","code":"60018","msg":"invalid channel","arg":{"instId":"ETH-USDT"}}`, streamMessage{}, errRequestRejected)
}

func TestProtocolSourceTimestampPriorityAndFallback(t *testing.T) {
	for _, fixture := range []struct {
		exchange string
		decode   func([]byte) (streamMessage, error)
		primary  string
		missing  string
		zero     string
	}{
		{
			exchange: "Bybit",
			decode:   decodeBybit,
			primary:  `{"cts":1700000000123,"ts":1700000000456,"data":{"s":"XAUTUSDT","b":[["99","1"]],"a":[["100","2"]]}}`,
			missing:  `{"ts":1700000000456,"data":{"s":"XAUTUSDT","b":[["99","1"]],"a":[["100","2"]]}}`,
			zero:     `{"cts":0,"ts":1700000000456,"data":{"s":"XAUTUSDT","b":[["99","1"]],"a":[["100","2"]]}}`,
		},
		{
			exchange: "Bitget",
			decode:   decodeBitget,
			primary:  `{"ts":1700000000456,"arg":{"symbol":"XAUTUSDT"},"data":[{"ts":"1700000000123","b":[["99","1"]],"a":[["100","2"]]},{"ts":"1700000000789","b":[["999","9"]],"a":[["1000","10"]]}]}`,
			missing:  `{"ts":1700000000456,"arg":{"symbol":"XAUTUSDT"},"data":[{"b":[["99","1"]],"a":[["100","2"]]}]}`,
			zero:     `{"ts":1700000000456,"arg":{"symbol":"XAUTUSDT"},"data":[{"ts":"0","b":[["99","1"]],"a":[["100","2"]]}]}`,
		},
	} {
		t.Run(fixture.exchange, func(t *testing.T) {
			for name, payload := range map[string]string{
				"primary": fixture.primary,
				"missing": fixture.missing,
				"zero":    fixture.zero,
			} {
				t.Run(name, func(t *testing.T) {
					message, err := fixture.decode([]byte(payload))
					if err != nil {
						t.Fatal(err)
					}

					nanos := 456000000
					if name == "primary" {
						nanos = 123000000
					}
					want := time.Date(2023, 11, 14, 22, 13, 20, nanos, time.UTC)

					if message.quote.Coin != "XAUT" || message.quote.Exchange != fixture.exchange || !message.quote.SourceAt.Equal(want) {
						t.Fatalf("wrong identity, timestamp priority or unit: %#v, want source_at %s", message.quote, want)
					}
				})
			}
		})
	}
}
