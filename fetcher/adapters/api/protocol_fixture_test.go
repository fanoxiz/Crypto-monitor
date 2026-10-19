package api

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/fanoxiz/crypto-monitor/fetcher/core"
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

func mutateDecoderMessage(t *testing.T, original []byte, mutate func(map[string]any)) []byte {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(original))
	decoder.UseNumber()
	var fields map[string]any

	if err := decoder.Decode(&fields); err != nil {
		t.Fatal(err)
	}

	mutate(fields)
	payload, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}

	return payload
}
