package adapters

import (
	"net/http"
	"fmt"
	"encoding/json"
	"strconv"
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

func (adap *BinanceAdapter) GetPrice(symbol string) (float64, error) {
	url := fmt.Sprintf("https://api.binance.com/api/v3/ticker/price?symbol=%sUSDT", symbol)

	resp, err := adap.client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("Wrong resp.StatusCode: %d", resp.StatusCode)
	}

	var apiResp struct {
		Symbol string `json:"symbol"`
		Price  string `json:"price"`
	}

	err = json.NewDecoder(resp.Body).Decode(&apiResp)
	if err != nil {
		return 0, err
	}
	price, err := strconv.ParseFloat(apiResp.Price, 64)
	if err != nil {
		return 0, err
	}
	return price, nil
}