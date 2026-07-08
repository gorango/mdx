package timeframe

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
)

type Timeframe struct {
	ID    string
	Ms    int64
	Label string
}

var tfRegex = regexp.MustCompile(`^([0-9]+)([mhdwM])$`)

func Parse(tf string) (Timeframe, error) {
	matches := tfRegex.FindStringSubmatch(tf)
	if len(matches) != 3 {
		return Timeframe{}, fmt.Errorf("invalid timeframe format: %s", tf)
	}

	val, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil || val <= 0 {
		return Timeframe{}, fmt.Errorf("invalid timeframe multiplier: %s", matches[1])
	}

	var unitMs int64
	var unitLabel string

	switch matches[2] {
	case "m":
		unitMs = 60_000
		unitLabel = "minute"
	case "h":
		unitMs = 3_600_000
		unitLabel = "hour"
	case "d":
		unitMs = 86_400_000
		unitLabel = "day"
	case "w":
		unitMs = 604_800_000
		unitLabel = "week"
	case "M":
		unitMs = 2_629_746_000
		unitLabel = "month"
	default:
		return Timeframe{}, fmt.Errorf("unsupported timeframe unit: %s", matches[2])
	}

	totalMs := val * unitMs
	label := fmt.Sprintf("%d %s", val, unitLabel)
	if val > 1 {
		label += "s"
	}

	return Timeframe{
		ID:    tf,
		Ms:    totalMs,
		Label: label,
	}, nil
}

func MustParse(tf string) Timeframe {
	t, err := Parse(tf)
	if err != nil {
		return TF1m
	}
	return t
}

func Duration(tf Timeframe) time.Duration {
	return time.Duration(tf.Ms) * time.Millisecond
}

var (
	TF1m  = Timeframe{"1m", 60_000, "1 minute"}
	TF5m  = Timeframe{"5m", 300_000, "5 minutes"}
	TF15m = Timeframe{"15m", 900_000, "15 minutes"}
	TF30m = Timeframe{"30m", 1_800_000, "30 minutes"}
	TF1h  = Timeframe{"1h", 3_600_000, "1 hour"}
	TF2h  = Timeframe{"2h", 7_200_000, "2 hours"}
	TF4h  = Timeframe{"4h", 14_400_000, "4 hours"}
	TF6h  = Timeframe{"6h", 21_600_000, "6 hours"}
	TF12h = Timeframe{"12h", 43_200_000, "12 hours"}
	TF1d  = Timeframe{"1d", 86_400_000, "1 day"}
	TF1w  = Timeframe{"1w", 604_800_000, "1 week"}
	TF1M  = Timeframe{"1M", 2_629_746_000, "1 month"}
)
