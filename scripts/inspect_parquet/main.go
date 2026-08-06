package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/parquet-go/parquet-go"
)

func main() {
	dtypes := []string{"open_interest", "liquidations", "trades", "orderbook"}

	for _, dtype := range dtypes {
		path := filepath.Join("/tmp", fmt.Sprintf("inspect_%s.parquet", dtype))
		f, err := os.Open(path)
		if err != nil {
			fmt.Printf("=== %s: cannot open file: %v\n\n", dtype, err)
			continue
		}

		st, _ := f.Stat()
		pf, err := parquet.OpenFile(f, st.Size())
		if err != nil {
			fmt.Printf("=== %s: cannot open parquet: %v\n\n", dtype, err)
			_ = f.Close()
			continue
		}

		fmt.Printf("=== %s ===\n", dtype)
		schema := pf.Schema()
		fmt.Printf("Schema columns: %v\n", schema.Columns())
		fmt.Printf("Num rows: %d\n", pf.NumRows())
		fmt.Printf("Num row groups: %d\n", len(pf.RowGroups()))

		// Find column indices for relevant fields
		columns := schema.Columns()
		colIndex := make(map[string]int)
		for i, col := range columns {
			colName := strings.ToLower(col[0])
			colIndex[colName] = i
		}

		// For orderbook, show more details
		if dtype == "orderbook" {
			fmt.Println("\nColumn indices:")
			for _, name := range []string{"event_time", "event_type", "side", "price", "quantity", "first_update_id", "final_update_id", "prev_final_update_id", "last_update_id"} {
				if idx, ok := colIndex[name]; ok {
					fmt.Printf("  %s: index %d\n", name, idx)
				}
			}

			// Collect unique event_types, sides and first 30 rows with their event_type
			eventTypes := make(map[string]int)
			sideValues := make(map[string]int)
			var sampleRows []string
			rowCount := 0
			var prevFinalUpdateID int64 = -1
			snapshotBoundaries := 0

			for _, rg := range pf.RowGroups() {
				rows := rg.Rows()
				buf := make([]parquet.Row, 100)
				for {
					n, err := rows.ReadRows(buf)
					if n == 0 || err != nil {
						if err != io.EOF && err != nil {
							fmt.Printf("read error: %v\n", err)
						}
						break
					}

					for i := 0; i < n; i++ {
						row := buf[i]
						rowCount++

						// Extract event_type if available
						if etIdx, ok := colIndex["event_type"]; ok && etIdx < len(row) {
							et := row[etIdx].String()
							eventTypes[et]++
						}

						// Extract side values
						if sideIdx, ok := colIndex["side"]; ok && sideIdx < len(row) {
							sv := row[sideIdx].String()
							sideValues[sv]++
						}

						// Check for snapshot boundaries (where prev_final_update_id doesn't match previous final_update_id)
						if prevFinalUpdateID >= 0 {
							if pfuIdx, ok := colIndex["prev_final_update_id"]; ok && pfuIdx < len(row) {
								currentPrevFinal := row[pfuIdx].Int64()
								if currentPrevFinal != prevFinalUpdateID {
									snapshotBoundaries++
								}
							}
						}
						if fuIdx, ok := colIndex["final_update_id"]; ok && fuIdx < len(row) {
							prevFinalUpdateID = row[fuIdx].Int64()
						}

						// Collect first 30 rows with details
						if len(sampleRows) < 30 {
							parts := []string{fmt.Sprintf("Row %d:", rowCount-1)}
							for _, colName := range []string{"event_time", "event_type", "side", "price", "quantity", "first_update_id", "final_update_id", "prev_final_update_id"} {
								if idx, ok := colIndex[colName]; ok && idx < len(row) {
									val := row[idx]
									switch colName {
									case "event_time":
										parts = append(parts, fmt.Sprintf("%s=%d", colName, val.Int64()))
									case "price", "quantity":
										parts = append(parts, fmt.Sprintf("%s=%s", colName, val.String()))
									case "first_update_id", "final_update_id", "prev_final_update_id":
										parts = append(parts, fmt.Sprintf("%s=%d", colName, val.Int64()))
									default:
										parts = append(parts, fmt.Sprintf("%s=%s", colName, val.String()))
									}
								}
							}
							sampleRows = append(sampleRows, strings.Join(parts, " "))
						}
					}

					if rowCount >= 10000 { // Limit scan for large files
						break
					}
				}
				_ = rows.Close()
				if rowCount >= 10000 {
					break
				}
			}

			fmt.Printf("\nSnapshot boundaries detected (prev_final mismatch): %d\n", snapshotBoundaries)

			fmt.Printf("\nUnique event_type values (scanned %d rows):\n", rowCount)
			for et, count := range eventTypes {
				fmt.Printf("  '%s': %d occurrences\n", et, count)
			}

			fmt.Printf("\nUnique side values (scanned %d rows):\n", rowCount)
			for sv, count := range sideValues {
				fmt.Printf("  '%s': %d occurrences\n", sv, count)
			}

			fmt.Println("\nFirst 30 rows:")
			for _, row := range sampleRows {
				fmt.Println(row)
			}
		} else {
			// For other dtypes, just show first 5 rows
			count := 0
			for _, rg := range pf.RowGroups() {
				rows := rg.Rows()
				buf := make([]parquet.Row, 1)
				for count < 5 {
					n, err := rows.ReadRows(buf)
					if n == 0 || err != nil {
						if err != io.EOF {
							fmt.Printf("read error: %v\n", err)
						}
						break
					}
					fmt.Printf("Row %d: %v\n", count, buf[0])
					count++
				}
				_ = rows.Close()
				if count >= 5 {
					break
				}
			}
		}

		fmt.Println()
		_ = f.Close()
	}
}
