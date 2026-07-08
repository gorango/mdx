package pipeline

import (
	"testing"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/orderbook/api"
)

func TestLastFundingRateAt(t *testing.T) {
	funding := []api.FundingPoint{
		{Time: 0, Rate: 0.0001},
		{Time: 8 * 3600000, Rate: 0.0002},
		{Time: 16 * 3600000, Rate: 0.0003},
		{Time: 24 * 3600000, Rate: 0.0004},
	}

	tests := []struct {
		name     string
		ts       int64
		wantRate *float64
	}{
		{
			name:     "before first funding point",
			ts:       -1000,
			wantRate: nil,
		},
		{
			name:     "exactly at first funding point",
			ts:       0,
			wantRate: ptr(0.0001),
		},
		{
			name:     "between first and second",
			ts:       4 * 3600000,
			wantRate: ptr(0.0001),
		},
		{
			name:     "exactly at second funding point",
			ts:       8 * 3600000,
			wantRate: ptr(0.0002),
		},
		{
			name:     "between second and third",
			ts:       12 * 3600000,
			wantRate: ptr(0.0002),
		},
		{
			name:     "exactly at last funding point",
			ts:       24 * 3600000,
			wantRate: ptr(0.0004),
		},
		{
			name:     "after last funding point",
			ts:       30 * 3600000,
			wantRate: ptr(0.0004),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &HourProcessor{fundingHistory: funding}
			got := p.lastFundingRateAt(tt.ts)
			if !ptrEq(got, tt.wantRate) {
				t.Errorf("lastFundingRateAt(%d): want %v, got %v", tt.ts, deref(tt.wantRate), deref(got))
			}
		})
	}
}

func TestLastFundingRateAt_EmptyHistory(t *testing.T) {
	p := &HourProcessor{fundingHistory: nil}
	if got := p.lastFundingRateAt(1000); got != nil {
		t.Errorf("want nil for nil history, got %v", *got)
	}

	p2 := &HourProcessor{fundingHistory: []api.FundingPoint{}}
	if got := p2.lastFundingRateAt(1000); got != nil {
		t.Errorf("want nil for empty slice, got %v", *got)
	}
}

func ptrEq(a, b *float64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func ptr(v float64) *float64 { return &v }

func deref(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

func cloneBars(bars []types.OrderbookBar) []types.OrderbookBar {
	result := make([]types.OrderbookBar, len(bars))
	copy(result, bars)
	return result
}

func TestProcessFundingHistory(t *testing.T) {
	funding := []api.FundingPoint{
		{Time: 0, Rate: 0.0001},
		{Time: 8 * 3600000, Rate: 0.0002},
		{Time: 16 * 3600000, Rate: 0.0003},
	}

	tests := []struct {
		name      string
		funding   []api.FundingPoint
		bars      []types.OrderbookBar
		wantRates []float64
	}{
		{
			name:      "bars before first funding point get no rate",
			funding:   funding,
			bars:      []types.OrderbookBar{{Timestamp: -1000}},
			wantRates: []float64{0},
		},
		{
			name:    "bars after first funding point get forward-filled rate",
			funding: funding,
			bars: []types.OrderbookBar{
				{Timestamp: 1 * 3600000},
				{Timestamp: 2 * 3600000},
				{Timestamp: 3 * 3600000},
			},
			wantRates: []float64{0.0001, 0.0001, 0.0001},
		},
		{
			name:    "rate changes at funding boundaries",
			funding: funding,
			bars: []types.OrderbookBar{
				{Timestamp: 7 * 3600000},
				{Timestamp: 8 * 3600000},
				{Timestamp: 9 * 3600000},
				{Timestamp: 16 * 3600000},
				{Timestamp: 17 * 3600000},
			},
			wantRates: []float64{0.0001, 0.0002, 0.0002, 0.0003, 0.0003},
		},
		{
			name:      "empty bars",
			funding:   funding,
			bars:      []types.OrderbookBar{},
			wantRates: nil,
		},
		{
			name:      "empty funding history leaves bars unchanged",
			funding:   []api.FundingPoint{},
			bars:      []types.OrderbookBar{{Timestamp: 1000}},
			wantRates: []float64{0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &HourProcessor{fundingHistory: tt.funding}
			got := p.processFundingHistory(cloneBars(tt.bars))

			if len(tt.wantRates) == 0 {
				return
			}

			for i, wantRate := range tt.wantRates {
				gotRate := got[i].FundingRate
				if wantRate == 0 {
					if gotRate != nil {
						t.Errorf("bar[%d] FundingRate: want nil, got %v", i, *gotRate)
					}
				} else {
					if gotRate == nil || *gotRate != wantRate {
						t.Errorf("bar[%d] FundingRate: want %v, got %v", i, wantRate, deref(gotRate))
					}
				}
			}

			for i := 1; i < len(got); i++ {
				if got[i].FundingRate == nil {
					continue
				}
				if got[i].FundingRateChange == nil {
					t.Errorf("bar[%d] FundingRateChange: want non-nil when rate is set, got nil", i)
				}
			}
		})
	}
}
