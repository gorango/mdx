package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"gorango/exchanges/domain/symbols"
)

type Manifest struct {
	Symbol          string    `json:"symbol"`
	StreamExchange  string    `json:"stream_exchange"`
	HydrateExchange string    `json:"hydrate_exchange"`
	CaptureStarted  time.Time `json:"capture_started"`
	CaptureEnded    time.Time `json:"capture_ended"`
	CompareStarted  time.Time `json:"compare_started"`
	CompareEnded    time.Time `json:"compare_ended"`
	Duration        string    `json:"duration"`
	Warmup          string    `json:"warmup"`
	OutputDir       string    `json:"output_dir"`
	ConfigPath      string    `json:"config_path"`
}

func main() {
	_ = godotenv.Load()

	var (
		symbol          = flag.String("symbol", "BTC/USDT:PERP", "Canonical symbol to compare")
		streamExchange  = flag.String("stream-exchange", "binance", "Exchange name used in live DB rows")
		hydrateExchange = flag.String("hydrate-exchange", "binance_futures", "Exchange name used by historical source")
		duration        = flag.Duration("duration", 2*time.Hour, "Live collection duration")
		warmup          = flag.Duration("warmup", 2*time.Minute, "Leading live capture period to exclude from comparison")
		outputDir       = flag.String("output-dir", "./parity-output", "Output directory for manifests and hydrate JSON")
		configPath      = flag.String("config", "config.yaml", "Config path for ob-stream")
		natsURL         = flag.String("nats", "nats://localhost:4222", "NATS URL passed to ob-stream")
		skipLive        = flag.Bool("skip-live", false, "Skip live collection and use explicit start/end")
		startStr        = flag.String("start", "", "UTC compare start for --skip-live, YYYY-MM-DDTHH:MM")
		endStr          = flag.String("end", "", "UTC compare end for --skip-live, YYYY-MM-DDTHH:MM")
		skipHydrate     = flag.Bool("skip-hydrate", false, "Skip historical hydrate and compare existing output")
		skipCompare     = flag.Bool("skip-compare", false, "Skip comparison after collection/hydration")
	)
	flag.Parse()

	if *duration <= 0 {
		log.Fatal("--duration must be positive")
	}
	if *warmup < 0 {
		log.Fatal("--warmup cannot be negative")
	}
	if *warmup >= *duration && !*skipLive {
		log.Fatal("--warmup must be shorter than --duration")
	}

	canonicalSymbol := symbols.NormalizeCanonical(*symbol)
	runID := time.Now().UTC().Format("20060102T150405Z")
	runDir := filepath.Join(*outputDir, runID)
	hydrateDir := filepath.Join(runDir, "hydrate")
	if err := os.MkdirAll(hydrateDir, 0o755); err != nil {
		log.Fatalf("create output dir: %v", err)
	}

	manifest := Manifest{
		Symbol:          canonicalSymbol,
		StreamExchange:  *streamExchange,
		HydrateExchange: *hydrateExchange,
		Duration:        duration.String(),
		Warmup:          warmup.String(),
		OutputDir:       runDir,
		ConfigPath:      *configPath,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *skipLive {
		start, err := parseUTCMinute(*startStr)
		if err != nil {
			log.Fatalf("invalid --start: %v", err)
		}
		end, err := parseUTCMinute(*endStr)
		if err != nil {
			log.Fatalf("invalid --end: %v", err)
		}
		if !end.After(start) {
			log.Fatal("--end must be after --start")
		}
		manifest.CaptureStarted = start
		manifest.CaptureEnded = end
		manifest.CompareStarted = start
		manifest.CompareEnded = end
	} else {
		if err := runLiveCapture(ctx, *configPath, *natsURL, *duration, &manifest); err != nil {
			log.Fatalf("live capture failed: %v", err)
		}
		manifest.CompareStarted = manifest.CaptureStarted.Add(*warmup).Truncate(time.Minute)
		manifest.CompareEnded = manifest.CaptureEnded.Truncate(time.Minute)
	}

	if err := writeManifest(runDir, manifest); err != nil {
		log.Fatalf("write manifest: %v", err)
	}

	if !*skipHydrate {
		if err := runHydrate(ctx, canonicalSymbol, *hydrateExchange, hydrateDir, manifest.CompareStarted, manifest.CompareEnded); err != nil {
			log.Fatalf("historical hydrate failed: %v", err)
		}
	}

	if !*skipCompare {
		if err := runCompare(ctx, canonicalSymbol, *streamExchange, *hydrateExchange, hydrateDir, manifest.CompareStarted, manifest.CompareEnded); err != nil {
			log.Fatalf("compare failed: %v", err)
		}
	}

	fmt.Printf("Parity run complete: %s\n", runDir)
}

func parseUTCMinute(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("value is required")
	}
	for _, layout := range []string{"2006-01-02T15:04", "2006-01-02T15"} {
		if t, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("expected YYYY-MM-DDTHH:MM or YYYY-MM-DDTHH")
}

func runLiveCapture(ctx context.Context, configPath, natsURL string, duration time.Duration, manifest *Manifest) error {
	manifest.CaptureStarted = time.Now().UTC()
	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/ob-stream", "--config", configPath, "--nats", natsURL)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return err
	}

	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}

	manifest.CaptureEnded = time.Now().UTC()
	if cmd.Process != nil {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGINT); err != nil {
			return err
		}
	}
	return cmd.Wait()
}

func runHydrate(ctx context.Context, symbol, exchange, outputDir string, start, end time.Time) error {
	args := []string{
		"run", "./cmd/ob-hydrate",
		"--symbol", symbol,
		"--exchange", exchange,
		"--start", formatMinute(start),
		"--end", formatMinute(end),
		"--dry-run",
		"--output-dir", outputDir,
	}
	return run(ctx, "go", args, nil)
}

func runCompare(ctx context.Context, symbol, streamExchange, hydrateExchange, inputDir string, start, end time.Time) error {
	env := append(os.Environ(),
		"HYDRATE_SYMBOL="+symbol,
		"HYDRATE_EXCHANGE="+streamExchange,
		"HYDRATE_INPUT_EXCHANGE="+hydrateExchange,
		"HYDRATE_INPUT_DIR="+inputDir,
		"HYDRATE_START="+formatMinute(start),
		"HYDRATE_END="+formatMinute(end),
	)
	return run(ctx, "go", []string{"run", "./cmd/ob-compare"}, env)
}

func run(ctx context.Context, name string, args []string, env []string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if env != nil {
		cmd.Env = env
	}
	return cmd.Run()
}

func writeManifest(runDir string, manifest Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(runDir, "manifest.json"), append(data, '\n'), 0o644)
}

func formatMinute(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04")
}
