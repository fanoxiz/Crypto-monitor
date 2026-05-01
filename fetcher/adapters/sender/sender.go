package sender

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

var jsonBufferPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

type SenderService struct {
	client   *http.Client
	endpoint string
}

func NewSenderService(client *http.Client, endpoint string) *SenderService {
	return &SenderService{
		client:   client,
		endpoint: endpoint,
	}
}

func (s *SenderService) Send(msg contracts.MarketTickerInfo) error {
	buf := jsonBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer jsonBufferPool.Put(buf)

	if err := json.NewEncoder(buf).Encode(msg); err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, s.endpoint, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return fmt.Errorf("create request error: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("analyzer returned wrong status: %d", resp.StatusCode)
	}

	return nil
}
