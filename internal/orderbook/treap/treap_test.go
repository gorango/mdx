package treap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	tree := New()
	assert.True(t, tree.IsEmpty())

	tree.Insert(100.0, 5.0)
	assert.False(t, tree.IsEmpty())
	assert.Equal(t, 5.0, tree.SumDepth())

	tree.Insert(100.0, 10.0)
	assert.Equal(t, 10.0, tree.SumDepth())

	tree.Insert(101.0, 3.0)
	assert.Equal(t, 13.0, tree.SumDepth())

	qty, ok := tree.Get(100.0)
	assert.True(t, ok)
	assert.Equal(t, 10.0, qty)
}

func TestRemove(t *testing.T) {
	tree := New()
	tree.Insert(100.0, 5.0)
	tree.Insert(101.0, 3.0)
	assert.Equal(t, 8.0, tree.SumDepth())

	tree.Remove(100.0)
	assert.Equal(t, 3.0, tree.SumDepth())

	_, ok := tree.Get(100.0)
	assert.False(t, ok)
}

func TestGet(t *testing.T) {
	tree := New()
	tree.Insert(100.0, 5.0)
	tree.Insert(102.0, 3.0)

	qty, ok := tree.Get(100.0)
	assert.True(t, ok)
	assert.Equal(t, 5.0, qty)

	_, ok = tree.Get(99.0)
	assert.False(t, ok)

	_, ok = tree.Get(103.0)
	assert.False(t, ok)
}

func TestMinMax(t *testing.T) {
	tree := New()
	assert.Equal(t, 0.0, tree.MinPrice())
	assert.Equal(t, 0.0, tree.MaxPrice())

	tree.Insert(100.0, 5.0)
	assert.Equal(t, 100.0, tree.MinPrice())
	assert.Equal(t, 100.0, tree.MaxPrice())

	tree.Insert(95.0, 3.0)
	assert.Equal(t, 95.0, tree.MinPrice())
	assert.Equal(t, 100.0, tree.MaxPrice())

	tree.Insert(105.0, 2.0)
	assert.Equal(t, 95.0, tree.MinPrice())
	assert.Equal(t, 105.0, tree.MaxPrice())
}

func TestDepthZero(t *testing.T) {
	tree := New()
	tree.Insert(100.0, 5.0)
	assert.Equal(t, 5.0, tree.SumDepth())
	assert.False(t, tree.IsEmpty())

	tree.Insert(100.0, 0.0)
	assert.Equal(t, 0.0, tree.SumDepth())
	assert.True(t, tree.IsEmpty())
}

func TestSumDepth(t *testing.T) {
	tree := New()
	assert.Equal(t, 0.0, tree.SumDepth())

	tree.Insert(100.0, 5.0)
	tree.Insert(101.0, 10.0)
	tree.Insert(102.0, 15.0)
	assert.Equal(t, 30.0, tree.SumDepth())

	tree.Insert(101.0, 8.0)
	assert.Equal(t, 28.0, tree.SumDepth())

	tree.Remove(100.0)
	assert.Equal(t, 23.0, tree.SumDepth())
}
