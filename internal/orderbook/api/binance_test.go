package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinanceClientNew(t *testing.T) {
	client := NewBinanceClient()
	assert.NotNil(t, client)
	assert.NotNil(t, client.httpClient)
}

func TestFundingPointStruct(t *testing.T) {
	point := FundingPoint{
		Time: 1234567890,
		Rate: 0.0001,
	}
	assert.Equal(t, int64(1234567890), point.Time)
	assert.InDelta(t, 0.0001, point.Rate, 0.00001)
}

func TestIsNotAvailableNil(t *testing.T) {
	assert.False(t, IsNotAvailable(nil))
}

func TestIsNotAvailableError(t *testing.T) {
	assert.True(t, IsNotAvailable(&netError{msg: "404: data not available"}))
	assert.True(t, IsNotAvailable(&netError{msg: "not available"}))
	assert.True(t, IsNotAvailable(&netError{msg: "not found"}))
	assert.True(t, IsNotAvailable(&netError{msg: "404 not found"}))
	assert.False(t, IsNotAvailable(&netError{msg: "server error"}))
	assert.False(t, IsNotAvailable(&netError{msg: "timeout"}))
}

func TestContains(t *testing.T) {
	assert.True(t, contains("hello world", "world"))
	assert.False(t, contains("hello", "world"))
	assert.True(t, contains("404: data not available", "404"))
	assert.False(t, contains("", "x"))
}

func TestContainsSubstring(t *testing.T) {
	assert.True(t, containsSubstring("hello world", "world"))
	assert.False(t, containsSubstring("hello", "world"))
	assert.True(t, containsSubstring("404: data", "404"))
	assert.False(t, containsSubstring("", "x"))
}

type netError struct {
	msg string
}

func (e *netError) Error() string {
	return e.msg
}
