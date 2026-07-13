package cache

import (
	"gorango/exchanges/domain/timeframe"
	"gorango/exchanges/domain/types"
	"sort"
	"time"
)

type Gap struct {
	Start time.Time
	End   time.Time
}

func FindGaps(existing []types.Bar, start, end time.Time, tfDef timeframe.Timeframe) []Gap {
	if len(existing) == 0 {
		return []Gap{{Start: start, End: end}}
	}

	tfDuration := time.Duration(tfDef.Ms) * time.Millisecond
	var gaps []Gap

	if existing[0].Time.After(start.Add(tfDuration)) {
		gaps = append(gaps, Gap{Start: start, End: existing[0].Time})
	}

	for i := 0; i < len(existing)-1; i++ {
		current := existing[i].Time
		next := existing[i+1].Time
		expectedNext := current.Add(tfDuration)

		if next.After(expectedNext) {
			gaps = append(gaps, Gap{Start: expectedNext, End: next})
		}
	}

	lastBar := existing[len(existing)-1]
	expectedLast := lastBar.Time.Add(tfDuration)
	if expectedLast.Before(end) {
		gaps = append(gaps, Gap{Start: expectedLast, End: end})
	}

	return gaps
}

func DeduplicateBars(bars []types.Bar) []types.Bar {
	if len(bars) == 0 {
		return bars
	}

	result := make([]types.Bar, 0, len(bars))
	result = append(result, bars[0])

	for i := 1; i < len(bars); i++ {
		if !bars[i].Time.Equal(result[len(result)-1].Time) {
			result = append(result, bars[i])
		}
	}

	return result
}

func AggregateBars(chunk []types.Bar) types.Bar {
	if len(chunk) == 0 {
		return types.Bar{}
	}
	if len(chunk) == 1 {
		return chunk[0]
	}

	open := chunk[0].Open
	high := chunk[0].High
	low := chunk[0].Low
	close := chunk[len(chunk)-1].Close
	var volume float64

	for _, b := range chunk {
		if b.High > high {
			high = b.High
		}
		if b.Low < low {
			low = b.Low
		}
		volume += b.Volume
	}

	return types.Bar{
		Time:   chunk[len(chunk)-1].Time,
		Open:   open,
		High:   high,
		Low:    low,
		Close:  close,
		Volume: volume,
	}
}

func ChunkDateRange(start, end time.Time) []string {
	if start.After(end) {
		return nil
	}

	var chunks []string
	current := start.UTC().Truncate(24 * time.Hour)
	endDay := end.UTC().Truncate(24 * time.Hour)

	for !current.After(endDay) {
		chunks = append(chunks, current.Format("2006-01-02"))
		current = current.Add(24 * time.Hour)
	}

	return chunks
}

func ParseChunkDate(dateStr string) (time.Time, error) {
	return time.Parse("2006-01-02", dateStr)
}

func ResampleBars(bars []types.Bar, targetTf timeframe.Timeframe) []types.Bar {
	if len(bars) == 0 {
		return nil
	}

	sourceTf := timeframe.TF1m
	if targetTf.Ms <= sourceTf.Ms {
		return copyBars(bars)
	}

	multiplier := targetTf.Ms / sourceTf.Ms
	if multiplier <= 1 {
		return copyBars(bars)
	}

	alignCloseTime := func(t time.Time, targetMs int64) time.Time {
		openMs := t.UnixMilli() - 60_000
		binnedOpenMs := (openMs / targetMs) * targetMs
		binnedCloseMs := binnedOpenMs + targetMs
		return time.UnixMilli(binnedCloseMs).UTC()
	}

	resampled := make([]types.Bar, 0, len(bars)/int(multiplier)+1)
	currentChunk := []types.Bar{bars[0]}
	currentAligned := alignCloseTime(bars[0].Time, targetTf.Ms)

	for i := 1; i < len(bars); i++ {
		barAligned := alignCloseTime(bars[i].Time, targetTf.Ms)
		if barAligned.Equal(currentAligned) {
			currentChunk = append(currentChunk, bars[i])
		} else {
			bar := AggregateBars(currentChunk)
			bar.Time = currentAligned
			resampled = append(resampled, bar)
			currentChunk = []types.Bar{bars[i]}
			currentAligned = barAligned
		}
	}
	if len(currentChunk) > 0 {
		bar := AggregateBars(currentChunk)
		bar.Time = currentAligned
		resampled = append(resampled, bar)
	}

	return resampled
}

func copyBars(bars []types.Bar) []types.Bar {
	result := make([]types.Bar, len(bars))
	copy(result, bars)
	return result
}

func SortBars(bars []types.Bar) {
	sort.Slice(bars, func(i, j int) bool {
		return bars[i].Time.Before(bars[j].Time)
	})
}
