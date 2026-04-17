package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type BybitAdapter struct {
	client *http.Client
}

func NewBybitAdapter(client *http.Client) *BybitAdapter {
	return &BybitAdapter{
		client: client,
	}
}

func (adap *BybitAdapter) GetName() string {
	return "Bybit"
}

func (adap *BybitAdapter) GetPrice(coinName string) (contracts.BidAsk, error) {
	url := fmt.Sprintf("https://api.bybit.com/v5/market/tickers?category=spot&symbol=%sUSDT", coinName)

	resp, err := adap.client.Get(url)
	if err != nil {
		return contracts.BidAsk{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return contracts.BidAsk{}, fmt.Errorf("wrong resp.StatusCode: %d", resp.StatusCode)
	}

	var apiResp struct {
		RetCode int    `json:"retCode"`
		RetMsg  string `json:"retMsg"`
		Result  struct {
			List []struct {
				Symbol    string `json:"symbol"`
				Bid1Price string `json:"bid1Price"`
				Ask1Price string `json:"ask1Price"`
			} `json:"list"`
		} `json:"result"`
	}

	err = json.NewDecoder(resp.Body).Decode(&apiResp)
	if err != nil {
		return contracts.BidAsk{}, err
	}

	if apiResp.RetCode != 0 {
		return contracts.BidAsk{}, fmt.Errorf("wrong resp.StatusCode: %d", apiResp.RetCode)
	}

	if len(apiResp.Result.List) == 0 {
		return contracts.BidAsk{}, fmt.Errorf("empty ticker list for coin: %s", coinName)
	}

	item := apiResp.Result.List[0]

	bid, err := strconv.ParseFloat(item.Bid1Price, 64)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("failed to parse bid price %q: %w", item.Bid1Price, err)
	}

	ask, err := strconv.ParseFloat(item.Ask1Price, 64)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("failed to parse ask price %q: %w", item.Ask1Price, err)
	}

	return contracts.BidAsk{Bid: bid, Ask: ask}, nil
}
