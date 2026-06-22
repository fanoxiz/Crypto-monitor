package core

import (
	"log"
	"math"
	"time"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

const overflowTimeout = 5 * time.Second

type WorkerPoolConfig struct {
	FetchWorkers    int
	SenderWorkers   int
	FetchQueueSize  int
	SenderQueueSize int
}

func (WorkerPoolConfig) Precalculate(coinsCount int, exchangesCount int, frequency time.Duration, httpTimeout time.Duration) WorkerPoolConfig {
	fetchRate := float64(coinsCount*exchangesCount) / frequency.Seconds()

	cfg := WorkerPoolConfig{
		FetchWorkers:    int(math.Ceil(fetchRate * httpTimeout.Seconds() / 2)),
		SenderWorkers:   int(fetchRate),
		FetchQueueSize:  int(math.Ceil(fetchRate * 10)),
		SenderQueueSize: int(math.Ceil(fetchRate * 10)),
	}
	log.Printf(
		"level=INFO component=core event=worker_pool_config fetch_workers=%d sender_workers=%d fetch_queue=%d sender_queue=%d",
		cfg.FetchWorkers,
		cfg.SenderWorkers,
		cfg.FetchQueueSize,
		cfg.SenderQueueSize,
	)
	return cfg
}

type fetchTask struct {
	coin string
	ex   ExchangeAdapter
}

type FetcherService struct {
	exchanges  []ExchangeAdapter
	sender     PriceSender
	fetchers   int
	senders    int
	fetchQueue chan fetchTask
	streamChan chan contracts.MarketTickerInfo
}

func NewFetcherService(exchanges []ExchangeAdapter, sender PriceSender, poolCfg WorkerPoolConfig) *FetcherService {
	return &FetcherService{
		exchanges:  exchanges,
		sender:     sender,
		fetchers:   poolCfg.FetchWorkers,
		senders:    poolCfg.SenderWorkers,
		fetchQueue: make(chan fetchTask, poolCfg.FetchQueueSize),
		streamChan: make(chan contracts.MarketTickerInfo, poolCfg.SenderQueueSize),
	}
}

func (s *FetcherService) Start(coins []string, freq time.Duration) {
	for range s.fetchers {
		go s.fetchWorker()
	}

	for range s.senders {
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
		log.Printf("level=WARN component=core event=fetch_queue_overflow coin=%s exchange=%s", coin, ex.GetName())
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
		s.streamChan <- contracts.MarketTickerInfo{
			CoinName:     coin,
			ExchangeName: ex.GetName(),
			Price: contracts.BidAsk{
				Bid: 1e9,
				Ask: 0,
			}, // затычка для сброса старой цены
		}
		log.Printf("level=ERROR component=core event=fetch_failed coin=%s exchange=%s err=\"%v\" ",
								coin, ex.GetName(), err)
		time.Sleep(overflowTimeout)
		return
	}

	s.streamChan <- contracts.MarketTickerInfo{
		CoinName:     coin,
		ExchangeName: ex.GetName(),
		Price:        price,
	}
}

func (s *FetcherService) senderWorker() {
	for msg := range s.streamChan {
		if err := s.sender.Send(msg); err != nil {
			log.Printf("level=ERROR component=core event=send_failed coin=%s exchange=%s err=\"%v \" ",
									msg.CoinName, msg.ExchangeName, err)
		}
	}
}
