package main

import (
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb"
)

const dbPath = "/mnt/c/Users/0510t/OneDrive/Documents/nat-gas-terminal/data/db/terminal.duckdb?access_mode=READ_ONLY"

func main() {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 1. What NWS series names actually exist in the DB?
	rows, err := db.Query("SELECT DISTINCT series_name FROM facts_time_series WHERE source_name = 'nws' ORDER BY series_name LIMIT 30")
	if err != nil {
		fmt.Println("series list error:", err)
	} else {
		fmt.Println("=== NWS series names in DB ===")
		for rows.Next() {
			var s string
			rows.Scan(&s)
			fmt.Println(" ", s)
		}
		rows.Close()
	}

	// 2. Try the summary query (GROUP BY before ORDER BY — DuckDB requires this order)
	fmt.Println("\n=== weather summary query ===")
	summarySQL := `
		SELECT
		    feature_date::VARCHAR,
		    MAX(CASE WHEN feature_name = 'weather_hdd_7d_weighted'    THEN value END),
		    MAX(CASE WHEN feature_name = 'weather_cdd_7d_weighted'    THEN value END),
		    MAX(CASE WHEN feature_name = 'weather_hdd_revision_delta' THEN value END),
		    MAX(computed_at)
		FROM features_daily
		WHERE feature_name IN (
		        'weather_hdd_7d_weighted',
		        'weather_cdd_7d_weighted',
		        'weather_hdd_revision_delta'
		      )
		  AND region = 'US'
		GROUP BY feature_date
		ORDER BY feature_date DESC
		LIMIT 1`
	row := db.QueryRow(summarySQL)
	var date, comp string
	var hdd, cdd, rev sql.NullFloat64
	if err := row.Scan(&date, &hdd, &cdd, &rev, &comp); err != nil {
		fmt.Println("error:", err)
	} else {
		fmt.Printf("date=%s hdd=%v cdd=%v rev=%v\n", date, hdd, cdd, rev)
	}

	// 3. NWS distinct series_name + region combos
	fmt.Println("\n=== NWS series_name + region ===")
	rows2, err := db.Query("SELECT DISTINCT series_name, region FROM facts_time_series WHERE source_name = 'nws' ORDER BY series_name, region LIMIT 40")
	if err != nil {
		fmt.Println("error:", err)
	} else {
		for rows2.Next() {
			var s, reg string
			rows2.Scan(&s, &reg)
			fmt.Printf("  series=%-30s region=%s\n", s, reg)
		}
		rows2.Close()
	}

	// 4. Try the new cities query
	fmt.Println("\n=== new cities join query ===")
	citiesSQL := `
		SELECT t.series_name, t.region, t.value, t.observation_time::TIMESTAMP::DATE::VARCHAR
		FROM facts_time_series t
		INNER JOIN (
		    SELECT series_name, region, MAX(observation_time) AS max_time
		    FROM facts_time_series
		    WHERE source_name = 'nws'
		      AND series_name IN ('forecast_hdd_65','forecast_cdd_65','forecast_temp_f')
		      AND region IN ('chicago','new_york','boston','atlanta','dallas','denver','minneapolis','detroit')
		    GROUP BY series_name, region
		) latest ON t.series_name = latest.series_name
		       AND t.region       = latest.region
		       AND t.observation_time = latest.max_time
		       AND t.source_name = 'nws'
		ORDER BY t.region, t.series_name`
	rows3, err := db.Query(citiesSQL)
	if err != nil {
		fmt.Println("error:", err)
	} else {
		for rows3.Next() {
			var s, reg, d string
			var v sql.NullFloat64
			rows3.Scan(&s, &reg, &v, &d)
			fmt.Printf("  %s / %s = %v (%s)\n", reg, s, v, d)
		}
		rows3.Close()
	}
}
