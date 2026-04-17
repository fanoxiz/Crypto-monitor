package receiver

import (
	"encoding/json"
	"log"
	"net/http"

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

	log.Printf("Executor запущен на порту %s", port)
	return http.ListenAndServe(":"+port, mux)
}

func (rec *HTTPReceiver) handleDeals(w http.ResponseWriter, r *http.Request) {
	var msg contracts.ProfitDealInfo

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if err := rec.executor.ProcessDeal(msg); err != nil {
		log.Printf("Ошибка обработки сделки: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
