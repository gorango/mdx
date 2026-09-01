package api

import (
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/h2non/gock"
	"github.com/stretchr/testify/assert"
)

func TestDownloadParquet404(t *testing.T) {
	defer gock.Off()

	gock.New("https://api.cryptohftdata.com").
		Get("/v1/download").
		Reply(http.StatusNotFound).BodyString("404: data not available")

	client := NewCryptoHFTClient("test-api-key")
	result, err := client.DownloadParquet("binance", "BTCUSDT", "2024-01-01", "00", "trades")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "download failed after retries")
}

func TestDownloadParquetHTTPError(t *testing.T) {
	defer gock.Off()

	gock.New("https://api.cryptohftdata.com").
		Get("/v1/download").
		ReplyError(io.EOF)

	client := NewCryptoHFTClient("test-api-key")
	result, err := client.DownloadParquet("binance", "BTCUSDT", "2024-01-01", "00", "trades")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "download failed after retries")
}

func TestDownloadResultCleanup(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_*.parquet")
	assert.NoError(t, err)
	_ = tmpFile.Close()
	tmpPath := tmpFile.Name()

	result := &DownloadResult{
		FilePath: tmpPath,
		Cleanup:  func() error { return os.Remove(tmpPath) },
	}

	err = result.Cleanup()
	assert.NoError(t, err)
	_, err = os.Stat(tmpPath)
	assert.True(t, os.IsNotExist(err))
}

func TestCryptoHFTClientNew(t *testing.T) {
	client := NewCryptoHFTClient("test-key")
	assert.NotNil(t, client)
	assert.Equal(t, "test-key", client.apiKey)
	assert.NotNil(t, client.httpClient)
}

func TestDownloadResultStruct(t *testing.T) {
	result := &DownloadResult{
		FilePath: "/tmp/test.parquet",
		Cleanup:  func() error { return nil },
	}
	assert.Equal(t, "/tmp/test.parquet", result.FilePath)
	assert.NotNil(t, result.Cleanup)

	err := result.Cleanup()
	assert.NoError(t, err)
}
