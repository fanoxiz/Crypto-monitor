package main

import (
	"log"
	"os"
	"strings"

	"github.com/fanoxiz/crypto-monitor/executor/adapters/receiver"
	"github.com/fanoxiz/crypto-monitor/executor/core"
)

func main() {
	port := strings.TrimSpace(os.Getenv("EXECUTOR_PORT"))
	if port == "" {
		port = "8082"
	}

	executorService := core.NewExecutorService()
	receiver := receiver.NewHTTPReceiver(executorService)

	if err := receiver.Start(port); err != nil {
		log.Fatalf("Ошибка запуска Executor: %v", err)
	}
}
