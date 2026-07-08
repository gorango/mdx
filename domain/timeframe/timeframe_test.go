package timeframe

import (
	"testing"
)

func TestParseStandard(t *testing.T) {
	tests := []struct {
		input string
		want  Timeframe
	}{
		{"1m", TF1m},
		{"5m", TF5m},
		{"15m", TF15m},
		{"30m", TF30m},
		{"1h", TF1h},
		{"2h", TF2h},
		{"4h", TF4h},
		{"6h", TF6h},
		{"12h", TF12h},
		{"1d", TF1d},
		{"1w", TF1w},
		{"1M", TF1M},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input)
			if err != nil {
				t.Errorf("Parse(%q) unexpected error: %v", tt.input, err)
				return
			}
			if got.ID != tt.want.ID || got.Ms != tt.want.Ms || got.Label != tt.want.Label {
				t.Errorf("Parse(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseDynamic(t *testing.T) {
	tests := []struct {
		input string
		want  Timeframe
	}{
		{"3m", Timeframe{"3m", 180_000, "3 minutes"}},
		{"7m", Timeframe{"7m", 420_000, "7 minutes"}},
		{"45m", Timeframe{"45m", 2_700_000, "45 minutes"}},
		{"3h", Timeframe{"3h", 10_800_000, "3 hours"}},
		{"5h", Timeframe{"5h", 18_000_000, "5 hours"}},
		{"8h", Timeframe{"8h", 28_800_000, "8 hours"}},
		{"3d", Timeframe{"3d", 259_200_000, "3 days"}},
		{"2w", Timeframe{"2w", 1_209_600_000, "2 weeks"}},
		{"3M", Timeframe{"3M", 7_889_238_000, "3 months"}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input)
			if err != nil {
				t.Errorf("Parse(%q) unexpected error: %v", tt.input, err)
				return
			}
			if got.ID != tt.want.ID || got.Ms != tt.want.Ms || got.Label != tt.want.Label {
				t.Errorf("Parse(%q) = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseInvalid(t *testing.T) {
	inputs := []string{
		"", "xyz", "1", "m", "0m", "-5m", "1y", "1.5h", "1mm",
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			got, err := Parse(input)
			if err == nil {
				t.Errorf("Parse(%q) expected error, got %+v", input, got)
			}
		})
	}
}

func TestMustParseFallback(t *testing.T) {
	got := MustParse("invalid_tf")
	if got.ID != TF1m.ID || got.Ms != TF1m.Ms {
		t.Errorf("MustParse('invalid_tf') = %+v, want %+v", got, TF1m)
	}
}

func TestMustParseDynamic(t *testing.T) {
	got := MustParse("3m")
	if got.ID != "3m" || got.Ms != 180_000 {
		t.Errorf("MustParse('3m') = %+v, want Timeframe{ID: '3m', Ms: 180000}", got)
	}
}

func TestMustParseStandard(t *testing.T) {
	got := MustParse("1h")
	if got.ID != TF1h.ID || got.Ms != TF1h.Ms {
		t.Errorf("MustParse('1h') = %+v, want %+v", got, TF1h)
	}
}

func TestDuration(t *testing.T) {
	tf, _ := Parse("5m")
	d := Duration(tf)
	if d.String() != "5m0s" {
		t.Errorf("Duration(5m) = %v, want 5m0s", d)
	}
}

func TestEdgeCases(t *testing.T) {
	got, err := Parse("1m")
	if err != nil {
		t.Fatal(err)
	}
	if got.Label != "1 minute" {
		t.Errorf("Label for 1m = %q, want '1 minute'", got.Label)
	}

	got2, err := Parse("1h")
	if err != nil {
		t.Fatal(err)
	}
	if got2.Label != "1 hour" {
		t.Errorf("Label for 1h = %q, want '1 hour'", got2.Label)
	}

	got3, err := Parse("1d")
	if err != nil {
		t.Fatal(err)
	}
	if got3.Label != "1 day" {
		t.Errorf("Label for 1d = %q, want '1 day'", got3.Label)
	}

	got4, err := Parse("1w")
	if err != nil {
		t.Fatal(err)
	}
	if got4.Label != "1 week" {
		t.Errorf("Label for 1w = %q, want '1 week'", got4.Label)
	}

	got5, err := Parse("1M")
	if err != nil {
		t.Fatal(err)
	}
	if got5.Label != "1 month" {
		t.Errorf("Label for 1M = %q, want '1 month'", got5.Label)
	}
}
