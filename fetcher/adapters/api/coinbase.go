package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type CoinbaseAdapter struct {
	client *http.Client
}

func NewCoinbaseAdapter(client *http.Client) *CoinbaseAdapter {
	return &CoinbaseAdapter{
		client: client,
	}
}

func (adap *CoinbaseAdapter) GetName() string {
	return "Coinbase"
}

func (adap *CoinbaseAdapter) GetPrice(coinName string) (contracts.BidAsk, error) {
	url := fmt.Sprintf("https://api.exchange.coinbase.com/products/%s-USDT/ticker", coinName)

	resp, err := adap.client.Get(url)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("coinbase request failed for %s: %w", coinName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return contracts.BidAsk{}, fmt.Errorf("wrong resp.StatusCode: %d", resp.StatusCode)
	}

	var apiResp struct {
		Bid string `json:"bid"`
		Ask string `json:"ask"`
	}

	err = json.NewDecoder(resp.Body).Decode(&apiResp)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("coinbase decode response failed for %s: %w", coinName, err)
	}

	if apiResp.Bid == "" || apiResp.Ask == "" {
		return contracts.BidAsk{}, fmt.Errorf("empty bid/ask for product: %s", coinName)
	}

	bid, err := strconv.ParseFloat(apiResp.Bid, 64)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("failed to parse bid price %q: %w", apiResp.Bid, err)
	}

	ask, err := strconv.ParseFloat(apiResp.Ask, 64)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("failed to parse ask price %q: %w", apiResp.Ask, err)
	}

	return contracts.BidAsk{Bid: bid, Ask: ask}, nil
}
