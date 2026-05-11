package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type BitgetAdapter struct {
	client *http.Client
}

func NewBitgetAdapter(client *http.Client) *BitgetAdapter {
	return &BitgetAdapter{
		client: client,
	}
}

func (adap *BitgetAdapter) GetName() string {
	return "Bitget"
}

func (adap *BitgetAdapter) GetPrice(coinName string) (contracts.BidAsk, error) {
	url := fmt.Sprintf("https://api.bitget.com/api/v2/spot/market/tickers?symbol=%sUSDT", coinName)

	resp, err := adap.client.Get(url)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("bitget get price: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return contracts.BidAsk{}, fmt.Errorf("bitget get price: unexpected http status: %d", resp.StatusCode)
	}

	var apiResp struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			Symbol string `json:"symbol"`
			BidPr  string `json:"bidPr"`
			AskPr  string `json:"askPr"`
		} `json:"data"`
	}

	err = json.NewDecoder(resp.Body).Decode(&apiResp)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("bitget get price: decode response: %w", err)
	}

	if apiResp.Code != "00000" {
		return contracts.BidAsk{}, fmt.Errorf("bitget get price: api error: code=%s msg=%s", apiResp.Code, apiResp.Msg)
	}

	if len(apiResp.Data) == 0 {
		return contracts.BidAsk{}, fmt.Errorf("bitget get price: empty data: symbol=%s", coinName)
	}

	bid, err := strconv.ParseFloat(apiResp.Data[0].BidPr, 64)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("bitget get price: parse bid price %q: %w", apiResp.Data[0].BidPr, err)
	}

	ask, err := strconv.ParseFloat(apiResp.Data[0].AskPr, 64)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("bitget get price: parse ask price %q: %w", apiResp.Data[0].AskPr, err)
	}

	return contracts.BidAsk{Bid: bid, Ask: ask}, nil
}
