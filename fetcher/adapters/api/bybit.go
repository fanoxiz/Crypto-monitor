package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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

func (adap *BybitAdapter) GetPrice(coinName string) (float64, error) {
	url := fmt.Sprintf("https://api.bybit.com/v5/market/tickers?category=spot&symbol=%sUSDT", coinName)

	resp, err := adap.client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("wrong resp.StatusCode: %d", resp.StatusCode)
	}

	var apiResp struct {
		RetCode int    `json:"retCode"`
		RetMsg  string `json:"retMsg"`
		Result  struct {
			List []struct {
				Symbol    string `json:"symbol"`
				LastPrice string `json:"lastPrice"`
			} `json:"list"`
		} `json:"result"`
	}

	err = json.NewDecoder(resp.Body).Decode(&apiResp)
	if err != nil {
		return 0, err
	}

	if apiResp.RetCode != 0 {
		return 0, fmt.Errorf("wrong resp.StatusCode: %d", apiResp.RetCode)
	}

	item := apiResp.Result.List[0]

	price, err := strconv.ParseFloat(item.LastPrice, 64)
	if err != nil {
		return 0, err
	}

	return price, nil
}
