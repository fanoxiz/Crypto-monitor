package core

import (
	"log"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

const fetchWorkers = 40
const senderWorkers = 20
const fetchQueueSize = 500
const senderQueueSize = 100

type fetchTask struct {
	coin string
	ex   ExchangeAdapter
}

type FetcherService struct {
	exchanges  []ExchangeAdapter
	sender     PriceSender
	fetchQueue chan fetchTask
	streamChan chan contracts.MarketTickerInfo
}

func NewFetcherService(exchanges []ExchangeAdapter, sender PriceSender) *FetcherService {
	return &FetcherService{
		exchanges:  exchanges,
		sender:     sender,
		fetchQueue: make(chan fetchTask, fetchQueueSize),
		streamChan: make(chan contracts.MarketTickerInfo, senderQueueSize),
	}
}

func (s *FetcherService) Start(coins []string, freq time.Duration) {
	for range fetchWorkers {
		go s.fetchWorker()
	}

	for range senderWorkers {
		go s.senderWorker()
	}

	ticker := time.NewTicker(freq)
	defer ticker.Stop()

	for {
		<-ticker.C
		for _, coin := range coins {
			for _, ex := range s.exchanges {
				s.enqueueFetchTask(coin, ex)
			}
		}
	}
}

func (s *FetcherService) enqueueFetchTask(coin string, ex ExchangeAdapter) {
	task := fetchTask{
		coin: coin,
		ex:   ex,
	}

	select {
	case s.fetchQueue <- task:
	default:
		log.Printf("Fetch queue overflow, skip task: coin=%s exchange=%s", coin, ex.GetName())
	}
}

func (s *FetcherService) fetchWorker() {
	for task := range s.fetchQueue {
		s.fetchSingle(task.coin, task.ex)
	}
}

func (s *FetcherService) fetchSingle(coin string, ex ExchangeAdapter) {
	price, err := ex.GetPrice(coin)
	if err != nil {
		log.Printf("[%s] Ошибка на %s: %v", coin, ex.GetName(), err)
		return
	}

	msg := contracts.MarketTickerInfo{
		CoinName:     coin,
		ExchangeName: ex.GetName(),
		Price:        price,
	}

	s.streamChan <- msg
}

func (s *FetcherService) senderWorker() {
	for msg := range s.streamChan {
		if err := s.sender.Send(msg); err != nil {
			log.Printf("Ошибка при отправке в Analyzer: %v", err)
		}
	}
}
