package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/fanoxiz/crypto-monitor/contracts"
)

type OKXAdapter struct {
	client *http.Client
}

func NewOKXAdapter(client *http.Client) *OKXAdapter {
	return &OKXAdapter{
		client: client,
	}
}

func (adap *OKXAdapter) GetName() string {
	return "OKX"
}

func (adap *OKXAdapter) GetPrice(coinName string) (contracts.BidAsk, error) {
	url := fmt.Sprintf("https://www.okx.com/api/v5/market/ticker?instId=%s-USDT", coinName)

	resp, err := adap.client.Get(url)
	if err != nil {
		return contracts.BidAsk{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return contracts.BidAsk{}, fmt.Errorf("wrong resp.StatusCode: %d", resp.StatusCode)
	}

	var apiResp struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			InstID string `json:"instId"`
			BidPx  string `json:"bidPx"`
			AskPx  string `json:"askPx"`
		} `json:"data"`
	}

	err = json.NewDecoder(resp.Body).Decode(&apiResp)
	if err != nil {
		return contracts.BidAsk{}, err
	}

	if apiResp.Code != "0" {
		return contracts.BidAsk{}, fmt.Errorf("okx error code: %s, msg: %s", apiResp.Code, apiResp.Msg)
	}

	if len(apiResp.Data) == 0 {
		return contracts.BidAsk{}, fmt.Errorf("empty data for instId: %s", coinName)
	}

	bid, err := strconv.ParseFloat(apiResp.Data[0].BidPx, 64)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("failed to parse bid price %q: %w", apiResp.Data[0].BidPx, err)
	}

	ask, err := strconv.ParseFloat(apiResp.Data[0].AskPx, 64)
	if err != nil {
		return contracts.BidAsk{}, fmt.Errorf("failed to parse ask price %q: %w", apiResp.Data[0].AskPx, err)
	}

	return contracts.BidAsk{Bid: bid, Ask: ask}, nil
}
