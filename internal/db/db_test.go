package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDBInvalidConnString(t *testing.T) {
	_, err := New("invalid-conn-string")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "parse config")
}

func TestDBClose(t *testing.T) {
	database := &DB{}
	database.Close()
}

func TestDBPoolNil(t *testing.T) {
	database := &DB{}
	pool := database.Pool()
	assert.Nil(t, pool)
}
