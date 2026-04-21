package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type BinanceAdapter struct {
	client *http.Client
}

func NewBinanceAdapter(client *http.Client) *BinanceAdapter {
	return &BinanceAdapter{
		client: client,
	}
}

func (adap *BinanceAdapter) GetName() string {
	return "Binance"
}

func (adap *BinanceAdapter) GetPrice(coinName string) (contracts.BidAsk, error) {
	url := fmt.Sprintf("https://api.binance.com/api/v3/ticker/bookTicker?symbol=%sUSDT", coinName)

	resp, err := adap.client.Get(url)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("binance request failed for %s: %w", coinName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return contracts.BidAsk{}, fmt.Errorf("wrong resp.StatusCode: %d", resp.StatusCode)
	}

	var apiResp struct {
		Symbol   string `json:"symbol"`
		BidPrice string `json:"bidPrice"`
		AskPrice string `json:"askPrice"`
	}

	err = json.NewDecoder(resp.Body).Decode(&apiResp)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("binance decode response failed for %s: %w", coinName, err)
	}

	bid, err := strconv.ParseFloat(apiResp.BidPrice, 64)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("failed to parse bid price %q: %w", apiResp.BidPrice, err)
	}

	ask, err := strconv.ParseFloat(apiResp.AskPrice, 64)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("failed to parse ask price %q: %w", apiResp.AskPrice, err)
	}

	return contracts.BidAsk{Bid: bid, Ask: ask}, nil
}
