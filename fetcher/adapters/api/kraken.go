//go:build false

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

type KrakenAdapter struct {
	client *http.Client
}

func NewKrakenAdapter(client *http.Client) *KrakenAdapter {
	return &KrakenAdapter{
		client: client,
	}
}

func (adap *KrakenAdapter) GetName() string {
	return "Kraken"
}

func (adap *KrakenAdapter) GetPrice(coinName string) (float64, error) {
	pair := fmt.Sprintf("%sUSDT", strings.ToUpper(coinName))
	url := fmt.Sprintf("https://api.kraken.com/0/public/Ticker?pair=%s", pair)

	resp, err := adap.client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("Wrong resp.StatusCode: %d", resp.StatusCode)
	}

	var apiResp struct {
		Error  []string `json:"error"`
		Result map[string]struct {
			C []string `json:"c"`
		} `json:"result"`
	}

	err = json.NewDecoder(resp.Body).Decode(&apiResp)
	if err != nil {
		return 0, err
	}

	if len(apiResp.Error) > 0 {
		return 0, fmt.Errorf("kraken error: %s", strings.Join(apiResp.Error, ", "))
	}

	item, ok := apiResp.Result[pair]
	if !ok {
		for _, v := range apiResp.Result {
			item = v
			ok = true
			break
		}
	}

	if !ok || len(item.C) == 0 {
		return 0, fmt.Errorf("empty ticker data for pair: %s", pair)
	}

	price, err := strconv.ParseFloat(item.C[0], 64)
	if err != nil {
		return 0, err
	}

	return price, nil
}
