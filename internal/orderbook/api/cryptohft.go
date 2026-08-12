package api

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/valyala/fasthttp"
)

type CryptoHFTClient struct {
	apiKey     string
	httpClient *fasthttp.Client
}

func NewCryptoHFTClient(apiKey string) *CryptoHFTClient {
	return &CryptoHFTClient{
		apiKey: apiKey,
		httpClient: &fasthttp.Client{
			ReadTimeout:     30 * time.Second,
			WriteTimeout:    30 * time.Second,
			MaxConnDuration: 5 * time.Minute,
		},
	}
}

type DownloadResult struct {
	FilePath string
	Cleanup  func() error
}

func (c *CryptoHFTClient) DownloadParquet(exchange, symbol, date, hour, dataType string) (*DownloadResult, error) {
	filePath := fmt.Sprintf("%s/%s/%s/%s_%s.parquet.zst", exchange, date, hour, symbol, dataType)
	url := fmt.Sprintf("https://api.cryptohftdata.com/download?file=%s&api_key=%s", filePath, c.apiKey)
	tempDir := os.TempDir()
	tempFile := filepath.Join(tempDir, fmt.Sprintf("%s_%s_%s_%s_%d.parquet", symbol, dataType, date, hour, time.Now().UnixNano()))
	var body []byte
	var err error
	for retries := 0; retries < 3; retries++ {
		body, err = c.downloadWithRetry(url)
		if err == nil {
			break
		}
		if strings.Contains(err.Error(), "404") ||
			strings.Contains(err.Error(), "400") ||
			strings.Contains(err.Error(), "not available") {
			break
		}
		time.Sleep(time.Duration(1<<retries) * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("download failed after retries: %w", err)
	}
	if len(body) < 100 {
		return nil, fmt.Errorf("data not available: %s", string(body))
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}
	defer decoder.Close()
	decompressed, err := decoder.DecodeAll(body, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress zstd: %w", err)
	}
	if err := os.WriteFile(tempFile, decompressed, 0644); err != nil {
		return nil, fmt.Errorf("write temp file: %w", err)
	}
	return &DownloadResult{FilePath: tempFile, Cleanup: func() error { return os.Remove(tempFile) }}, nil
}

func (c *CryptoHFTClient) downloadWithRetry(url string) ([]byte, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodGet)
	if err := c.httpClient.Do(req, resp); err != nil {
		return nil, err
	}
	if resp.StatusCode() == http.StatusNotFound {
		return nil, fmt.Errorf("404: data not available")
	}
	if resp.StatusCode() >= 400 && resp.StatusCode() < 500 {
		return nil, fmt.Errorf("%d: data not available", resp.StatusCode())
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode(), resp.Body())
	}
	return resp.Body(), nil
}

func IsNotAvailable(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "404") || strings.Contains(errStr, "not available") || strings.Contains(errStr, "not found")
}
