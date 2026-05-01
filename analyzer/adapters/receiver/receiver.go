package receiver

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/fanoxiz/crypto-monitor/analyzer/core"
	"github.com/fanoxiz/crypto-monitor/contracts"
)

var jsonBufferPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

type HTTPReceiver struct {
	analyzer core.Analyzer
}

func NewHTTPReceiver(an core.Analyzer) *HTTPReceiver {
	return &HTTPReceiver{
		analyzer: an,
	}
}

func (rec *HTTPReceiver) Start(port string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /prices", rec.handlePrices)
	log.Printf("Analyzer запущен на порту %s", port)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return srv.ListenAndServe()
}

func (rec *HTTPReceiver) handlePrices(w http.ResponseWriter, r *http.Request) {
	var msg contracts.MarketTickerInfo

	buf := jsonBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufferPool.Put(buf)

	if _, err := buf.ReadFrom(r.Body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := json.Unmarshal(buf.Bytes(), &msg); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := rec.analyzer.ProcessPrices(msg); err != nil {
		log.Printf("Ошибка анализа: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
