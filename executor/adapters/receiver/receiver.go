package receiver

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
	"github.com/fanoxiz/crypto-monitor/executor/core"
)

type HTTPReceiver struct {
	executor core.Executor
}

func NewHTTPReceiver(executor core.Executor) *HTTPReceiver {
	return &HTTPReceiver{executor: executor}
}

func (rec *HTTPReceiver) Start(port string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /deals", rec.handleDeals)
	mux.HandleFunc("GET /stats", rec.handleGetStats)
	log.Printf("Executor запущен на порту %s", port)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return srv.ListenAndServe()
}

func (rec *HTTPReceiver) handleDeals(w http.ResponseWriter, r *http.Request) {
	var msg contracts.ProfitDealInfo

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := rec.executor.ProcessDeal(r.Context(), msg); err != nil {
		log.Printf("Ошибка обработки сделки: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (rec *HTTPReceiver) handleGetStats(w http.ResponseWriter, r *http.Request) {
	stats, err := rec.executor.GetStats(r.Context())
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(stats); err != nil {
		log.Printf("Ошибка формирования ответа stats: %v", err)
	}
}
