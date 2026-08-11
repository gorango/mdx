package cache

import (
	"gorango/mdx/domain/timeframe"
	"gorango/mdx/domain/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFindGaps(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	start := now
	end := now.Add(10 * time.Minute)

	bars := []types.Bar{
		{Time: now.Add(time.Minute), Open: 100, High: 101, Low: 99, Close: 100},
		{Time: now.Add(3 * time.Minute), Open: 100, High: 101, Low: 99, Close: 100},
	}

	gaps := FindGaps(bars, start, end, timeframe.TF1m)

	assert.GreaterOrEqual(t, len(gaps), 1)
}

func TestFindGapsNoExistingBars(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	start := now
	end := now.Add(10 * time.Minute)

	gaps := FindGaps([]types.Bar{}, start, end, timeframe.TF1m)

	assert.Equal(t, 1, len(gaps))
	assert.Equal(t, start, gaps[0].Start)
	assert.Equal(t, end, gaps[0].End)
}

// TestResampleBars1mTo5m uses 1m source bars with close times
func TestResampleBars1mTo5m_Standalone(t *testing.T) {
	bars := []types.Bar{
		{Time: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
		{Time: time.Date(2024, 1, 1, 0, 2, 0, 0, time.UTC), Open: 103, High: 106, Low: 102, Close: 104, Volume: 1100},
		{Time: time.Date(2024, 1, 1, 0, 3, 0, 0, time.UTC), Open: 104, High: 107, Low: 103, Close: 105, Volume: 1200},
		{Time: time.Date(2024, 1, 1, 0, 4, 0, 0, time.UTC), Open: 105, High: 108, Low: 104, Close: 106, Volume: 1300},
		{Time: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC), Open: 106, High: 109, Low: 105, Close: 107, Volume: 1400},
	}

	resampled := ResampleBars(bars, timeframe.TF5m)

	assert.Equal(t, 1, len(resampled))
	assert.Equal(t, time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC), resampled[0].Time)
	assert.Equal(t, 100.0, resampled[0].Open)
	assert.Equal(t, 109.0, resampled[0].High)
	assert.Equal(t, 99.0, resampled[0].Low)
	assert.Equal(t, 107.0, resampled[0].Close)
	assert.Equal(t, 6000.0, resampled[0].Volume)
}

func TestFindGapsNoGaps(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	start := now
	end := now.Add(3 * time.Minute)

	bars := []types.Bar{
		{Time: now.Add(1 * time.Minute), Open: 100, High: 101, Low: 99, Close: 100},
		{Time: now.Add(2 * time.Minute), Open: 100, High: 101, Low: 99, Close: 100},
		{Time: now.Add(3 * time.Minute), Open: 100, High: 101, Low: 99, Close: 100},
	}

	gaps := FindGaps(bars, start, end, timeframe.TF1m)

	assert.Equal(t, 0, len(gaps))
}

func TestDeduplicateBars(t *testing.T) {
	now := time.Now()
	bars := []types.Bar{
		{Time: now, Open: 100, High: 101, Low: 99, Close: 100},
		{Time: now, Open: 100, High: 101, Low: 99, Close: 100},
		{Time: now.Add(time.Minute), Open: 101, High: 102, Low: 100, Close: 101},
	}

	deduped := DeduplicateBars(bars)

	assert.Equal(t, 2, len(deduped))
}

func TestDeduplicateBarsEmpty(t *testing.T) {
	deduped := DeduplicateBars([]types.Bar{})
	assert.Equal(t, 0, len(deduped))
}

func TestResampleBars1mTo1h(t *testing.T) {
	bars := make([]types.Bar, 60)
	for i := 0; i < 60; i++ {
		bars[i] = types.Bar{
			Time:   time.Date(2024, 1, 1, 0, i+1, 0, 0, time.UTC),
			Open:   float64(100 + i),
			High:   float64(101 + i),
			Low:    float64(99 + i),
			Close:  float64(100.5 + float64(i)),
			Volume: float64(1000 + i),
		}
	}

	resampled := ResampleBars(bars, timeframe.TF1h)

	assert.Equal(t, 1, len(resampled))
	assert.Equal(t, time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), resampled[0].Time)
	assert.Equal(t, 100.0, resampled[0].Open)
	assert.Equal(t, 160.0, resampled[0].High)
	assert.Equal(t, 99.0, resampled[0].Low)
	assert.Equal(t, 159.5, resampled[0].Close)
}

func TestResampleBarsEmpty_Standalone(t *testing.T) {
	resampled := ResampleBars([]types.Bar{}, timeframe.TF5m)
	assert.Nil(t, resampled)
}

func TestResampleBarsSameTimeframe(t *testing.T) {
	bars := []types.Bar{
		{Time: time.Now(), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
	}

	resampled := ResampleBars(bars, timeframe.TF1m)
	assert.Equal(t, 1, len(resampled))
	assert.Equal(t, bars[0].Time, resampled[0].Time)
}

func TestResampleBarsLowerTarget_Standalone(t *testing.T) {
	bars := []types.Bar{
		{Time: time.Now(), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
	}

	resampled := ResampleBars(bars, timeframe.TF1m)
	assert.Equal(t, bars, resampled)
}

func TestResampleBarsPartialGroup_Standalone(t *testing.T) {
	bars := []types.Bar{
		{Time: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC), Open: 100, High: 101, Low: 99, Close: 100, Volume: 100},
		{Time: time.Date(2024, 1, 1, 0, 2, 0, 0, time.UTC), Open: 100, High: 101, Low: 99, Close: 100, Volume: 100},
		{Time: time.Date(2024, 1, 1, 0, 3, 0, 0, time.UTC), Open: 100, High: 101, Low: 99, Close: 100, Volume: 100},
	}

	resampled := ResampleBars(bars, timeframe.TF5m)

	assert.Equal(t, 1, len(resampled))
	assert.Equal(t, time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC), resampled[0].Time)
	assert.Equal(t, 100.0, resampled[0].Open)
	assert.Equal(t, 101.0, resampled[0].High)
	assert.Equal(t, 99.0, resampled[0].Low)
	assert.Equal(t, 100.0, resampled[0].Close)
}

func TestResampleBarsTimestampAlignment(t *testing.T) {
	bars := []types.Bar{
		{Time: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC), Open: 100, High: 105, Low: 99, Close: 103, Volume: 1000},
		{Time: time.Date(2024, 1, 1, 0, 6, 0, 0, time.UTC), Open: 103, High: 106, Low: 102, Close: 104, Volume: 1100},
		{Time: time.Date(2024, 1, 1, 0, 7, 0, 0, time.UTC), Open: 104, High: 107, Low: 103, Close: 105, Volume: 1200},
		{Time: time.Date(2024, 1, 1, 0, 8, 0, 0, time.UTC), Open: 105, High: 108, Low: 104, Close: 106, Volume: 1300},
		{Time: time.Date(2024, 1, 1, 0, 9, 0, 0, time.UTC), Open: 106, High: 109, Low: 105, Close: 107, Volume: 1400},
	}

	resampled := ResampleBars(bars, timeframe.TF1h)

	assert.Equal(t, 1, len(resampled))
	assert.Equal(t, 1, resampled[0].Time.Hour())
	assert.Equal(t, 0, resampled[0].Time.Minute())
	assert.Equal(t, 0, resampled[0].Time.Second())
	assert.Equal(t, 0, resampled[0].Time.Nanosecond())
}

func TestResampleBarsTimestampAlignmentMultipleHours(t *testing.T) {
	bars := make([]types.Bar, 180)
	for i := 0; i < 180; i++ {
		bars[i] = types.Bar{
			Time:   time.Date(2024, 1, 1, 0, i+1, 0, 0, time.UTC),
			Open:   float64(100 + i),
			High:   float64(101 + i),
			Low:    float64(99 + i),
			Close:  float64(100.5 + float64(i)),
			Volume: float64(1000 + i),
		}
	}

	resampled := ResampleBars(bars, timeframe.TF1h)

	assert.Equal(t, 3, len(resampled))
	for i, bar := range resampled {
		assert.Equal(t, 0, bar.Time.Minute(), "Hour %d should start at minute 0", i)
		assert.Equal(t, 0, bar.Time.Second(), "Hour %d should start at second 0", i)
		assert.Equal(t, i+1, bar.Time.Hour(), "Hour %d should be at hour %d", i, i+1)
	}
}

func TestChunkDateRange(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 3, 23, 59, 59, 0, time.UTC)

	chunks := ChunkDateRange(start, end)

	assert.Equal(t, 3, len(chunks))
	assert.Equal(t, "2024-01-01", chunks[0])
	assert.Equal(t, "2024-01-02", chunks[1])
	assert.Equal(t, "2024-01-03", chunks[2])
}

func TestChunkDateRangeSingleDay(t *testing.T) {
	start := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 18, 0, 0, 0, time.UTC)

	chunks := ChunkDateRange(start, end)

	assert.Equal(t, 1, len(chunks))
	assert.Equal(t, "2024-01-01", chunks[0])
}

func TestChunkDateRangeInvalid(t *testing.T) {
	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	chunks := ChunkDateRange(start, end)

	assert.Nil(t, chunks)
}

func TestParseChunkDate(t *testing.T) {
	dateStr := "2024-01-15"
	parsed, err := ParseChunkDate(dateStr)

	assert.NoError(t, err)
	assert.Equal(t, 2024, parsed.Year())
	assert.Equal(t, time.January, parsed.Month())
	assert.Equal(t, 15, parsed.Day())
}

func TestFindGapsSparseHourlyBars(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	bars := make([]types.Bar, 1000)
	for i := 0; i < 1000; i++ {
		bars[i] = types.Bar{
			Time:   start.Add(time.Duration(i+1) * time.Hour),
			Open:   100,
			High:   101,
			Low:    99,
			Close:  100,
			Volume: 1000,
		}
	}
	end := start.Add(2000 * time.Hour)
	gaps := FindGaps(bars, start, end, timeframe.TF1m)
	assert.Equal(t, 1001, len(gaps), "1000 hourly bars create 1001 tiny gaps (start gap + 999 inter-bar + trailing)")
}

func TestSortBars(t *testing.T) {
	now := time.Now()
	bars := []types.Bar{
		{Time: now.Add(time.Hour), Open: 100},
		{Time: now, Open: 101},
		{Time: now.Add(2 * time.Hour), Open: 102},
	}

	SortBars(bars)

	assert.Equal(t, now, bars[0].Time)
	assert.Equal(t, now.Add(time.Hour), bars[1].Time)
	assert.Equal(t, now.Add(2*time.Hour), bars[2].Time)
}

func TestResampleBars1mTo3m(t *testing.T) {
	bars := make([]types.Bar, 9)
	for i := 0; i < 9; i++ {
		bars[i] = types.Bar{
			Time:   time.Date(2024, 1, 1, 0, i+1, 0, 0, time.UTC),
			Open:   float64(100 + i),
			High:   float64(101 + i),
			Low:    float64(99 + i),
			Close:  float64(100.5 + float64(i)),
			Volume: float64(1000 + i),
		}
	}

	targetTf := timeframe.Timeframe{ID: "3m", Ms: 180_000, Label: "3 minutes"}
	resampled := ResampleBars(bars, targetTf)

	assert.Equal(t, 3, len(resampled))

	assert.Equal(t, time.Date(2024, 1, 1, 0, 3, 0, 0, time.UTC), resampled[0].Time)
	assert.Equal(t, 100.0, resampled[0].Open)
	assert.Equal(t, 103.0, resampled[0].High)
	assert.Equal(t, 99.0, resampled[0].Low)
	assert.Equal(t, 102.5, resampled[0].Close)
	assert.Equal(t, 3003.0, resampled[0].Volume)

	assert.Equal(t, time.Date(2024, 1, 1, 0, 6, 0, 0, time.UTC), resampled[1].Time)
	assert.Equal(t, 103.0, resampled[1].Open)
	assert.Equal(t, 106.0, resampled[1].High)
	assert.Equal(t, 102.0, resampled[1].Low)
	assert.Equal(t, 105.5, resampled[1].Close)
	assert.Equal(t, 3012.0, resampled[1].Volume)

	assert.Equal(t, time.Date(2024, 1, 1, 0, 9, 0, 0, time.UTC), resampled[2].Time)
	assert.Equal(t, 106.0, resampled[2].Open)
	assert.Equal(t, 109.0, resampled[2].High)
	assert.Equal(t, 105.0, resampled[2].Low)
	assert.Equal(t, 108.5, resampled[2].Close)
	assert.Equal(t, 3021.0, resampled[2].Volume)
}

func TestResampleBars1mTo7m(t *testing.T) {
	bars := make([]types.Bar, 14)
	for i := 0; i < 14; i++ {
		bars[i] = types.Bar{
			Time:   time.Date(2024, 1, 1, 0, i+1, 0, 0, time.UTC),
			Open:   float64(100 + i),
			High:   float64(101 + i),
			Low:    float64(99 + i),
			Close:  float64(100.5 + float64(i)),
			Volume: float64(1000 + i),
		}
	}

	// 7m intervals from Unix epoch: midnight is NOT a bucket boundary,
	// so the first bar (close 0:01) opens at 0:00 and falls into a
	// pre-midnight partial bucket closing at 0:01.
	targetTf := timeframe.Timeframe{ID: "7m", Ms: 420_000, Label: "7 minutes"}
	resampled := ResampleBars(bars, targetTf)

	assert.Equal(t, 3, len(resampled))

	// Partial bucket: open 0:00 falls before the first full 7m block
	assert.Equal(t, time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC), resampled[0].Time)
	assert.Equal(t, 100.0, resampled[0].Open)
	assert.Equal(t, 101.0, resampled[0].High)
	assert.Equal(t, 99.0, resampled[0].Low)
	assert.Equal(t, 100.5, resampled[0].Close)
	assert.Equal(t, 1000.0, resampled[0].Volume)

	// Full bucket: bars 0:02-0:08 (7 bars)
	assert.Equal(t, time.Date(2024, 1, 1, 0, 8, 0, 0, time.UTC), resampled[1].Time)
	assert.Equal(t, 101.0, resampled[1].Open)
	assert.Equal(t, 108.0, resampled[1].High)
	assert.Equal(t, 100.0, resampled[1].Low)
	assert.Equal(t, 107.5, resampled[1].Close)

	// Partial bucket: bars 0:09-0:14 (6 of 7 bars)
	assert.Equal(t, time.Date(2024, 1, 1, 0, 15, 0, 0, time.UTC), resampled[2].Time)
	assert.Equal(t, 108.0, resampled[2].Open)
	assert.Equal(t, 114.0, resampled[2].High)
	assert.Equal(t, 107.0, resampled[2].Low)
	assert.Equal(t, 113.5, resampled[2].Close)
}

func TestResampleBars1mTo5h(t *testing.T) {
	bars := make([]types.Bar, 600)
	for i := 0; i < 600; i++ {
		bars[i] = types.Bar{
			Time:   time.Date(2024, 1, 1, 0, i+1, 0, 0, time.UTC),
			Open:   float64(100 + i),
			High:   float64(101 + i),
			Low:    float64(99 + i),
			Close:  float64(100.5 + float64(i)),
			Volume: float64(1000 + i),
		}
	}

	// 5h intervals from Unix epoch: midnight is NOT a bucket boundary.
	// Buckets: close 3:00, 8:00, 13:00, ...
	targetTf := timeframe.Timeframe{ID: "5h", Ms: 18_000_000, Label: "5 hours"}
	resampled := ResampleBars(bars, targetTf)

	assert.Equal(t, 3, len(resampled))

	assert.Equal(t, time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC), resampled[0].Time)
	assert.Equal(t, 100.0, resampled[0].Open)
	assert.Equal(t, 280.0, resampled[0].High)
	assert.Equal(t, 99.0, resampled[0].Low)
	assert.Equal(t, 279.5, resampled[0].Close)
	assert.Equal(t, 196110.0, resampled[0].Volume)

	assert.Equal(t, time.Date(2024, 1, 1, 8, 0, 0, 0, time.UTC), resampled[1].Time)
	assert.Equal(t, 280.0, resampled[1].Open)
	assert.Equal(t, 580.0, resampled[1].High)
	assert.Equal(t, 279.0, resampled[1].Low)
	assert.Equal(t, 579.5, resampled[1].Close)
	assert.Equal(t, 398850.0, resampled[1].Volume)

	assert.Equal(t, time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC), resampled[2].Time)
	assert.Equal(t, 580.0, resampled[2].Open)
	assert.Equal(t, 700.0, resampled[2].High)
	assert.Equal(t, 579.0, resampled[2].Low)
	assert.Equal(t, 699.5, resampled[2].Close)
	assert.Equal(t, 184740.0, resampled[2].Volume)
}

func TestResampleBarsPartialGroupDynamic(t *testing.T) {
	bars := []types.Bar{
		{Time: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC), Open: 100, High: 101, Low: 99, Close: 100, Volume: 100},
		{Time: time.Date(2024, 1, 1, 0, 2, 0, 0, time.UTC), Open: 100, High: 101, Low: 99, Close: 100, Volume: 100},
		{Time: time.Date(2024, 1, 1, 0, 3, 0, 0, time.UTC), Open: 100, High: 101, Low: 99, Close: 100, Volume: 100},
		{Time: time.Date(2024, 1, 1, 0, 4, 0, 0, time.UTC), Open: 100, High: 101, Low: 99, Close: 100, Volume: 100},
	}

	targetTf := timeframe.Timeframe{ID: "3m", Ms: 180_000, Label: "3 minutes"}
	resampled := ResampleBars(bars, targetTf)

	assert.Equal(t, 2, len(resampled))

	assert.Equal(t, time.Date(2024, 1, 1, 0, 3, 0, 0, time.UTC), resampled[0].Time)
	assert.Equal(t, 100.0, resampled[0].Open)
	assert.Equal(t, 101.0, resampled[0].High)
	assert.Equal(t, 99.0, resampled[0].Low)
	assert.Equal(t, 100.0, resampled[0].Close)
	assert.Equal(t, 300.0, resampled[0].Volume)

	assert.Equal(t, time.Date(2024, 1, 1, 0, 6, 0, 0, time.UTC), resampled[1].Time)
	assert.Equal(t, 100.0, resampled[1].Open)
	assert.Equal(t, 101.0, resampled[1].High)
	assert.Equal(t, 99.0, resampled[1].Low)
	assert.Equal(t, 100.0, resampled[1].Close)
	assert.Equal(t, 100.0, resampled[1].Volume)
}
