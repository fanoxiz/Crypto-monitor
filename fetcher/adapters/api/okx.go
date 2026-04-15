package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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

func (adap *OKXAdapter) GetPrice(coinName string) (float64, error) {
	instID := fmt.Sprintf("%s-USDT", strings.ToUpper(coinName))
	url := fmt.Sprintf("https://www.okx.com/api/v5/market/ticker?instId=%s", instID)

	resp, err := adap.client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("wrong resp.StatusCode: %d", resp.StatusCode)
	}

	var apiResp struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			InstID string `json:"instId"`
			Last   string `json:"last"`
		} `json:"data"`
	}

	err = json.NewDecoder(resp.Body).Decode(&apiResp)
	if err != nil {
		return 0, err
	}

	if apiResp.Code != "0" {
		return 0, fmt.Errorf("okx error code: %s, msg: %s", apiResp.Code, apiResp.Msg)
	}

	if len(apiResp.Data) == 0 {
		return 0, fmt.Errorf("empty data for instId: %s", instID)
	}

	price, err := strconv.ParseFloat(apiResp.Data[0].Last, 64)
	if err != nil {
		return 0, err
	}

	return price, nil
}
