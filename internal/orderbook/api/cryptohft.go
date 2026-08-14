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

	// Retry must cover the zstd decode too: under parallel load the vendor/CDN
	// can return HTTP 200 with a corrupt or truncated body (rate-limit HTML,
	// partial frame).  Those only fail at the DECOMPRESS step, after the HTTP
	// layer has accepted them — previously they were reported once as a data
	// gap instead of being re-fetched.  Only a genuine 404 / not-available is
	// terminal.
	var body, decompressed []byte
	var err error
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		body, err = c.downloadWithRetry(url)
		if err != nil {
			if IsNotAvailable(err) {
				// Terminal (404 / not-available): match the historical error
				// contract — wrapped in "download failed after retries".
				return nil, fmt.Errorf("download failed after retries: %w", err)
			}
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}
		if len(body) < 100 {
			// Error-page sized payload (e.g. an auth/rate-limit page) — treat
			// as not available, not a data gap.
			return nil, fmt.Errorf("data not available: %s", string(body))
		}
		decoder, zerr := zstd.NewReader(nil)
		if zerr != nil {
			return nil, fmt.Errorf("create zstd decoder: %w", zerr)
		}
		decompressed, err = decoder.DecodeAll(body, nil)
		decoder.Close()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(1<<attempt) * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("download failed after retries: %w", err)
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
	if resp.StatusCode() == http.StatusTooManyRequests || resp.StatusCode() >= 500 {
		// Rate limit / transient server error — retryable, NOT a data gap.
		// (IsNotAvailable does not match these.)
		return nil, fmt.Errorf("HTTP %d: transient", resp.StatusCode())
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("%d: data not available", resp.StatusCode())
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
