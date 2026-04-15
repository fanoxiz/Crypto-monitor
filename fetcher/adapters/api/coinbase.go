package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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

func (adap *CoinbaseAdapter) GetPrice(coinName string) (float64, error) {
	productID := fmt.Sprintf("%s-USDT", strings.ToUpper(coinName))
	url := fmt.Sprintf("https://api.exchange.coinbase.com/products/%s/ticker", productID)

	resp, err := adap.client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("Wrong resp.StatusCode: %d", resp.StatusCode)
	}

	var apiResp struct {
		Price string `json:"price"`
	}

	err = json.NewDecoder(resp.Body).Decode(&apiResp)
	if err != nil {
		return 0, err
	}

	if apiResp.Price == "" {
		return 0, fmt.Errorf("empty price for product: %s", productID)
	}

	price, err := strconv.ParseFloat(apiResp.Price, 64)
	if err != nil {
		return 0, err
	}

	return price, nil
}
