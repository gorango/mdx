package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"gorango/exchanges/domain/symbols"
	"gorango/exchanges/domain/types"
	"gorango/exchanges/internal/db"
	"gorango/exchanges/internal/orderbook/aggregator"
	"gorango/exchanges/internal/orderbook/api"
	"gorango/exchanges/internal/orderbook/parquet"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

func isDuplicateKeyError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "23505")
}

type HourResult struct {
	Date             string
	Hour             int
	BarsSaved        int
	TradesCount      int
	BytesFetched     int64
	Skipped          bool
	DataNotAvailable bool
	Error            error
}

type HourProcessor struct {
	apiClient       *api.CryptoHFTClient
	db              *db.DB
	symbol          string
	exchange        string
	logger          *slog.Logger
	fundingHistory  []api.FundingPoint
	agg             *aggregator.Aggregator
	dryRun          bool
	outputDir       string
	prevFundingRate *float64
	overwrite       bool
}

func NewHourProcessor(apiKey, symbol, exchange string, database *db.DB, fundingHistory []api.FundingPoint, overwrite bool) *HourProcessor {
	return &HourProcessor{
		apiClient:      api.NewCryptoHFTClient(apiKey),
		db:             database,
		symbol:         symbol,
		exchange:       exchange,
		logger:         newLogger(),
		fundingHistory: fundingHistory,
		agg:            aggregator.New(),
		overwrite:      overwrite,
	}
}

func (p *HourProcessor) SetFundingHistory(fundingHistory []api.FundingPoint) {
	p.fundingHistory = fundingHistory
}

func (p *HourProcessor) SetDryRun(dryRun bool, outputDir string) {
	p.dryRun = dryRun
	p.outputDir = outputDir
}

func (p *HourProcessor) Process(ctx context.Context, date string, hour int) (*HourResult, error) {
	hourStr := fmt.Sprintf("%02d", hour)
	result := &HourResult{
		Date: date,
		Hour: hour,
	}

	if !p.dryRun && !p.overwrite {
		hourTime, _ := time.ParseInLocation("2006-01-02T15:04:05", date+"T"+hourStr+":00:00", time.UTC)
		dbExchange := symbols.MapExchangeToDB(p.exchange)
		exists, err := p.db.HourExists(ctx, dbExchange, p.symbol, hourTime)
		if err != nil {
			return nil, fmt.Errorf("check hour exists: %w", err)
		}
		if exists {
			p.logger.Debug("Hour already exists, skipping",
				"symbol", p.symbol,
				"date", date,
				"hour", hour,
			)
			result.Skipped = true
			return result, nil
		}
	}

	p.logger.Debug("Processing hour",
		"symbol", p.symbol,
		"date", date,
		"hour", hour,
	)

	agg := p.agg

	tradesCount, err := p.processTrades(date, hourStr, agg)
	if err != nil {
		if api.IsNotAvailable(err) {
			p.logger.Error("Data not available (404), skipping hour",
				"symbol", p.symbol,
				"date", date,
				"hour", hour,
				"error", err,
			)
			result.Skipped = true
			result.DataNotAvailable = true
			return result, nil
		}
		return nil, fmt.Errorf("process trades: %w", err)
	}
	result.TradesCount = tradesCount

	p.logger.Debug("Trades processed",
		"symbol", p.symbol,
		"date", date,
		"hour", hour,
		"trades", tradesCount,
	)

	if err := p.processOpenInterest(date, hourStr, agg); err != nil {
	}

	liqSucceeded := false
	if err := p.processLiquidations(date, hourStr, agg); err != nil {
	} else {
		liqSucceeded = true
	}

	if err := p.processOrderBook(date, hourStr, agg); err != nil {
		p.logger.Debug("Orderbook error (non-fatal)",
			"symbol", p.symbol,
			"date", date,
			"hour", hour,
			"error", err,
		)
	}

	bars := agg.Finalize(liqSucceeded)

	if len(bars) > 1 {
		seen := make(map[int64]bool)
		deduped := bars[:0]
		for _, b := range bars {
			if !seen[b.Timestamp] {
				seen[b.Timestamp] = true
				deduped = append(deduped, b)
			}
		}
		if len(deduped) < len(bars) {
			p.logger.Debug("Deduplicated bars",
				"symbol", p.symbol,
				"date", date,
				"hour", hour,
				"before", len(bars),
				"after", len(deduped),
			)
			bars = deduped
		}
	}

	p.logger.Debug("Finalized bars",
		"symbol", p.symbol,
		"date", date,
		"hour", hour,
		"barCount", len(bars),
	)

	bars = p.processFundingHistory(bars)

	if len(bars) > 0 {
		if p.dryRun {
			if err := p.writeBarsToJSON(date, hourStr, bars); err != nil {
				return nil, fmt.Errorf("write bars to JSON: %w", err)
			}
		} else {
			dbExchange := symbols.MapExchangeToDB(p.exchange)
			if p.overwrite {
				hourTime, _ := time.ParseInLocation("2006-01-02T15:04:05", date+"T"+hourStr+":00:00", time.UTC)
				hourEnd := hourTime.Add(time.Hour)
				if _, err := p.db.DeleteOrderbookBars(ctx, dbExchange, p.symbol, &hourTime, &hourEnd); err != nil {
					return nil, fmt.Errorf("delete existing bars: %w", err)
				}
			}
			if err := p.db.InsertOrderbookBars(ctx, dbExchange, p.symbol, bars); err != nil {
				if isDuplicateKeyError(err) {
					result.Skipped = true
					result.BarsSaved = 0
					return result, nil
				}
				p.logger.Debug("Insert error",
					"symbol", p.symbol,
					"date", date,
					"hour", hour,
					"error", err.Error(),
				)
				return nil, fmt.Errorf("insert bars: %w", err)
			}
		}
	}

	result.BarsSaved = len(bars)

	p.logger.Debug("Hour complete",
		"symbol", p.symbol,
		"date", date,
		"hour", hour,
		"trades", tradesCount,
		"bars", len(bars),
	)

	return result, nil
}

func (p *HourProcessor) processTrades(date, hourStr string, agg *aggregator.Aggregator) (int, error) {
	exchangeSymbol := symbols.CanonicalToExchange(p.symbol, p.exchange)
	download, err := p.apiClient.DownloadParquet(p.exchange, exchangeSymbol, date, hourStr, "trades")
	if err != nil {
		return 0, err
	}
	defer download.Cleanup()

	reader, err := parquet.Open(download.FilePath)
	if err != nil {
		return 0, fmt.Errorf("open parquet: %w", err)
	}
	defer reader.Close()

	hourTime, _ := time.ParseInLocation("2006-01-02T15:04:05", date+"T"+hourStr+":00:00", time.UTC)
	hourStartMs := hourTime.UnixMilli()
	hourEndMs := hourStartMs + 3600000

	var count int
	err = reader.StreamTrades(func(t parquet.Trade) error {
		if t.TradeTime >= hourStartMs && t.TradeTime < hourEndMs {
			agg.ProcessTrade(aggregator.Trade{
				Timestamp:    t.TradeTime,
				Price:        t.Price,
				Quantity:     t.Quantity,
				IsBuyerMaker: t.IsBuyerMaker,
				TradeCount:   1,
			})
			count++
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("stream trades: %w", err)
	}

	return count, nil
}

func (p *HourProcessor) processOpenInterest(date, hourStr string, agg *aggregator.Aggregator) error {
	exchangeSymbol := symbols.CanonicalToExchange(p.symbol, p.exchange)
	download, err := p.apiClient.DownloadParquet(p.exchange, exchangeSymbol, date, hourStr, "open_interest")
	if err != nil {
		return err
	}
	defer download.Cleanup()

	reader, err := parquet.Open(download.FilePath)
	if err != nil {
		return fmt.Errorf("open parquet: %w", err)
	}
	defer reader.Close()

	hourTime, _ := time.ParseInLocation("2006-01-02T15:04:05", date+"T"+hourStr+":00:00", time.UTC)
	hourStartMs := hourTime.UnixMilli()
	hourEndMs := hourStartMs + 3600000

	return reader.StreamOpenInterest(func(oi parquet.OpenInterest) error {
		if oi.Timestamp >= hourStartMs && oi.Timestamp < hourEndMs {
			oiValue := oi.SumOpenInterest
			if oiValue == 0 {
				oiValue = oi.SumOpenInterestValue
			}
			agg.ProcessOpenInterest(aggregator.OpenInterest{
				Timestamp: oi.Timestamp,
				Value:     oiValue,
			})
		}
		return nil
	})
}

func (p *HourProcessor) processLiquidations(date, hourStr string, agg *aggregator.Aggregator) error {
	exchangeSymbol := symbols.CanonicalToExchange(p.symbol, p.exchange)
	download, err := p.apiClient.DownloadParquet(p.exchange, exchangeSymbol, date, hourStr, "liquidations")
	if err != nil {
		return err
	}
	defer download.Cleanup()

	reader, err := parquet.Open(download.FilePath)
	if err != nil {
		return fmt.Errorf("open parquet: %w", err)
	}
	defer reader.Close()

	hourTime, _ := time.ParseInLocation("2006-01-02T15:04:05", date+"T"+hourStr+":00:00", time.UTC)
	hourStartMs := hourTime.UnixMilli()
	hourEndMs := hourStartMs + 3600000

	return reader.StreamLiquidations(func(liq parquet.Liquidation) error {
		if liq.TradeTime >= hourStartMs && liq.TradeTime < hourEndMs {
			agg.ProcessLiquidation(aggregator.Liquidation{
				Timestamp: liq.TradeTime,
				Quantity:  liq.LastFilledQuantity,
				Side:      liq.Side,
			})
		}
		return nil
	})
}

func (p *HourProcessor) processOrderBook(date, hourStr string, agg *aggregator.Aggregator) error {
	exchangeSymbol := symbols.CanonicalToExchange(p.symbol, p.exchange)
	download, err := p.apiClient.DownloadParquet(p.exchange, exchangeSymbol, date, hourStr, "orderbook")
	if err != nil {
		return err
	}
	defer download.Cleanup()

	reader, err := parquet.Open(download.FilePath)
	if err != nil {
		return fmt.Errorf("open parquet: %w", err)
	}
	defer reader.Close()

	hourTime, _ := time.ParseInLocation("2006-01-02T15:04:05", date+"T"+hourStr+":00:00", time.UTC)
	hourStartMs := hourTime.UnixMilli()
	hourEndMs := hourStartMs + 3600000

	return reader.StreamOrderBook(func(update parquet.OrderBook) error {
		if update.EventTime >= hourStartMs && update.EventTime < hourEndMs {
			agg.ProcessOrderBookUpdate(aggregator.OrderBookUpdate{
				EventTime:         update.EventTime,
				Price:             update.Price,
				Quantity:          update.Quantity,
				Side:              update.Side,
				EventType:         update.EventType,
				FinalUpdateID:     update.FinalUpdateID,
				PrevFinalUpdateID: update.PrevFinalUpdateID,
			})
		}
		return nil
	})
}

func (p *HourProcessor) lastFundingRateAt(ts int64) *float64 {
	funding := p.fundingHistory
	if len(funding) == 0 {
		return nil
	}
	lo, hi := 0, len(funding)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if funding[mid].Time <= ts {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	if funding[lo].Time <= ts {
		return &funding[lo].Rate
	}
	return nil
}

func (p *HourProcessor) processFundingHistory(bars []types.OrderbookBar) []types.OrderbookBar {
	if len(p.fundingHistory) == 0 || len(bars) == 0 {
		return bars
	}
	missing := 0
	for i := range bars {
		rate := p.lastFundingRateAt(bars[i].Timestamp)
		bars[i].FundingRate = rate
		if rate != nil && p.prevFundingRate != nil {
			delta := *rate - *p.prevFundingRate
			bars[i].FundingRateChange = &delta
		}
		if rate != nil {
			p.prevFundingRate = rate
		}
		if rate == nil {
			missing++
		}
	}
	if len(bars) > 0 {
		missingPct := float64(missing) / float64(len(bars)) * 100
		if p.logger != nil {
			p.logger.Debug("Funding rate assignment",
				"symbol", p.symbol,
				"date", bars[0].Timestamp,
				"missing_pct", missingPct,
			)
			if missingPct > 50 {
				p.logger.Warn("High percentage of bars missing funding rate",
					"symbol", p.symbol,
					"missing_pct", missingPct,
					"total", len(bars),
				)
			}
		}
	}
	return bars
}

func (p *HourProcessor) writeBarsToJSON(date, hourStr string, bars []types.OrderbookBar) error {
	dir := filepath.Join(p.outputDir, p.exchange, p.symbol)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	filename := fmt.Sprintf("%s_%s.json", date, hourStr)
	path := filepath.Join(dir, filename)

	data, err := json.MarshalIndent(bars, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal bars: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write file: %w", err)
	}

	p.logger.Debug("Wrote bars to JSON",
		"symbol", p.symbol,
		"path", path,
		"barCount", len(bars),
	)

	return nil
}
