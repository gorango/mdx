package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/valyala/fasthttp"
)

type FundingPoint struct {
	Time int64
	Rate float64
}

type BinanceClient struct {
	httpClient *fasthttp.Client
}

func NewBinanceClient() *BinanceClient {
	return &BinanceClient{
		httpClient: &fasthttp.Client{
			ReadTimeout:     10 * time.Second,
			WriteTimeout:    10 * time.Second,
			MaxConnDuration: 5 * time.Minute,
		},
	}
}

func (c *BinanceClient) FetchFundingHistory(symbol string, startTime, endTime int64) ([]FundingPoint, error) {
	var allPoints []FundingPoint
	limit := 1000
	cursor := startTime

	for cursor <= endTime {
		points, err := c.fetchFundingPage(symbol, cursor, endTime, limit)
		if err != nil {
			return nil, fmt.Errorf("fetch funding page: %w", err)
		}
		if len(points) == 0 {
			break
		}
		allPoints = append(allPoints, points...)
		lastTime := points[len(points)-1].Time
		next := lastTime + 1
		if next <= cursor {
			break
		}
		cursor = next
		if len(points) < limit {
			break
		}
	}
	return allPoints, nil
}

func (c *BinanceClient) FetchLatestFundingRate(symbol string) (FundingPoint, error) {
	now := time.Now().UnixMilli()
	dayAgo := now - 24*3600000
	points, err := c.FetchFundingHistory(symbol, dayAgo, now)
	if err != nil {
		return FundingPoint{}, err
	}
	if len(points) == 0 {
		return FundingPoint{}, fmt.Errorf("no funding rates found for %s", symbol)
	}
	return points[len(points)-1], nil
}

func (c *BinanceClient) fetchFundingPage(symbol string, startTime, endTime int64, limit int) ([]FundingPoint, error) {
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/fundingRate?symbol=%s&startTime=%d&endTime=%d&limit=%d",
		symbol, startTime, endTime, limit)
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodGet)
	if err := c.httpClient.Do(req, resp); err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode(), resp.Body())
	}
	var rawData []struct {
		Symbol      string `json:"symbol"`
		FundingRate string `json:"fundingRate"`
		FundingTime int64  `json:"fundingTime"`
	}
	if err := json.Unmarshal(resp.Body(), &rawData); err != nil {
		return nil, fmt.Errorf("unmarshal funding data: %w", err)
	}
	points := make([]FundingPoint, len(rawData))
	for i, d := range rawData {
		rate := 0.0
		fmt.Sscanf(d.FundingRate, "%f", &rate)
		points[i] = FundingPoint{Time: d.FundingTime, Rate: rate}
	}
	return points, nil
}
