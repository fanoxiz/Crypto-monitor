package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

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

type subscriptionLogRecord struct {
	Message       string    `json:"msg"`
	Level         string    `json:"level"`
	Coin          string    `json:"coin"`
	Reason        string    `json:"reason"`
	Confirmed     bool      `json:"confirmed"`
	DataReceived  bool      `json:"data_received"`
	QuoteReceived bool      `json:"quote_received"`
	LastDataAt    time.Time `json:"last_data_at"`
	LastQuoteAt   time.Time `json:"last_quote_at"`
	Invalidated   bool      `json:"invalidated"`
	DataAge       *int64    `json:"data_age"`
	QuoteAge      *int64    `json:"quote_age"`
}

func subscriptionLogRecords(t *testing.T, logs *reconnectLogBuffer) []subscriptionLogRecord {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(logs.snapshot()))
	var records []subscriptionLogRecord

	for {
		var record subscriptionLogRecord
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			return records
		}
		if err != nil {
			t.Fatal(err)
		}

		records = append(records, record)
	}
}
