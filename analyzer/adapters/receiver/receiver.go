package receiver

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/fanoxiz/crypto-monitor/analyzer/core"
	"github.com/fanoxiz/crypto-monitor/contracts"
)

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
	return http.ListenAndServe(":"+port, mux)
}

func (rec *HTTPReceiver) handlePrices(w http.ResponseWriter, r *http.Request) {
	var msg contracts.MarketTickerInfo

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
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
