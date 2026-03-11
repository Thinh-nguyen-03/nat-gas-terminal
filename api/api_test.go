package main

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	appdb "github.com/nat-gas-terminal/api/internal/db"
	"github.com/nat-gas-terminal/api/internal/handler"
	"github.com/nat-gas-terminal/api/internal/sse"
)

// newTestDB creates a temporary DuckDB file, initialises the schema, seeds
// known rows, and returns a read-only connection to it.
// The returned cleanup func deletes the file when called.
func newTestDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	// Generate a unique temp path without creating the file.
	// DuckDB requires the file to not exist (or be a valid DB) when it opens it.
	// os.CreateTemp would create an empty file which DuckDB rejects as invalid.
	f, err := os.CreateTemp("", "terminal_test_*.duckdb")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	f.Close()
	path := f.Name()
	os.Remove(path) // remove so DuckDB can create a fresh DB at this path

	// Open read-write to seed the database.
	rw, err := sql.Open("duckdb", path)
	if err != nil {
		os.Remove(path)
		t.Fatalf("open rw duckdb: %v", err)
	}
	seedSchema(t, rw)
	seedData(t, rw)
	rw.Close()

	// Re-open read-only as the API would.
	ro, err := appdb.Open(path)
	if err != nil {
		os.Remove(path)
		t.Fatalf("open ro duckdb: %v", err)
	}
	return ro, func() {
		ro.Close()
		os.Remove(path)
	}
}

func seedSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS facts_time_series (
			source_name      VARCHAR NOT NULL,
			series_name      VARCHAR NOT NULL,
			region           VARCHAR NOT NULL DEFAULT 'US',
			observation_time TIMESTAMPTZ NOT NULL,
			release_time     TIMESTAMPTZ,
			ingest_time      TIMESTAMPTZ NOT NULL,
			value            DOUBLE,
			unit             VARCHAR,
			frequency        VARCHAR,
			quality_flag     VARCHAR DEFAULT 'ok',
			metadata_json    VARCHAR,
			PRIMARY KEY (source_name, series_name, region, observation_time)
		)`,
		`CREATE TABLE IF NOT EXISTS features_daily (
			feature_date   DATE NOT NULL,
			feature_name   VARCHAR NOT NULL,
			region         VARCHAR NOT NULL DEFAULT 'US',
			value          DOUBLE,
			interpretation VARCHAR,
			confidence     VARCHAR,
			computed_at    TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (feature_date, feature_name, region)
		)`,
		`CREATE TABLE IF NOT EXISTS summary_outputs (
			summary_date DATE NOT NULL,
			summary_type VARCHAR NOT NULL,
			content      VARCHAR NOT NULL,
			inputs_hash  VARCHAR,
			generated_at TIMESTAMPTZ NOT NULL,
			PRIMARY KEY (summary_date, summary_type)
		)`,
		`CREATE TABLE IF NOT EXISTS collector_health (
			source_name          VARCHAR NOT NULL PRIMARY KEY,
			last_attempt         TIMESTAMPTZ NOT NULL,
			last_success         TIMESTAMPTZ,
			last_status          VARCHAR NOT NULL,
			consecutive_failures INTEGER DEFAULT 0,
			error_message        VARCHAR
		)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("seed schema: %v\nSQL: %s", err, s)
		}
	}
}

func seedData(t *testing.T, db *sql.DB) {
	t.Helper()
	today := time.Now().UTC().Format("2006-01-02")
	now := time.Now().UTC().Format(time.RFC3339)

	// summary_outputs — fundamental score
	scoreJSON := `{"score":11.4,"label":"Mildly Bullish","drivers":["Storage 95 Bcf below 5yr avg (bullish)"]}`
	_, err := db.Exec(`
		INSERT INTO summary_outputs (summary_date, summary_type, content, generated_at)
		VALUES (?, 'fundamental_score', ?, ?)
	`, today, scoreJSON, now)
	if err != nil {
		t.Fatalf("seed score: %v", err)
	}

	// summary_outputs — what_changed
	changesJSON := `[{"feature":"storage_deficit_vs_5yr_bcf","current_value":-95.0,"prior_value":-80.0,"delta":-15.0,"significant":true}]`
	_, err = db.Exec(`
		INSERT INTO summary_outputs (summary_date, summary_type, content, generated_at)
		VALUES (?, 'what_changed', ?, ?)
	`, today, changesJSON, now)
	if err != nil {
		t.Fatalf("seed what_changed: %v", err)
	}

	// features_daily — storage features
	storageFeatures := []struct {
		name  string
		value float64
		interp string
		conf  string
	}{
		{"storage_total_bcf", 1755.0, "neutral", "high"},
		{"storage_deficit_vs_5yr_bcf", -95.0, "bullish", "high"},
		{"storage_wow_change_bcf", -110.0, "neutral", "high"},
		{"storage_eos_projection_bcf", 1820.0, "bullish", "medium"},
	}
	for _, f := range storageFeatures {
		_, err := db.Exec(`
			INSERT INTO features_daily (feature_date, feature_name, region, value, interpretation, confidence, computed_at)
			VALUES (?, ?, 'US', ?, ?, ?, ?)
		`, today, f.name, f.value, f.interp, f.conf, now)
		if err != nil {
			t.Fatalf("seed feature %s: %v", f.name, err)
		}
	}

	// features_daily — weather
	_, err = db.Exec(`
		INSERT INTO features_daily (feature_date, feature_name, region, value, interpretation, confidence, computed_at)
		VALUES (?, 'weather_hdd_7d_weighted', 'US', 62.5, 'bullish', 'high', ?)
	`, today, now)
	if err != nil {
		t.Fatalf("seed weather hdd: %v", err)
	}

	// facts_time_series — storage (for latestWeekEnding)
	_, err = db.Exec(`
		INSERT INTO facts_time_series
			(source_name, series_name, region, observation_time, ingest_time, value, unit, frequency)
		VALUES ('eia_storage', 'storage_total', 'total', '2026-03-01T00:00:00Z', ?, 1755.0, 'Bcf', 'weekly')
	`, now)
	if err != nil {
		t.Fatalf("seed eia_storage: %v", err)
	}

	// facts_time_series — eia_storage_stats (5yr band)
	_, err = db.Exec(`
		INSERT INTO facts_time_series
			(source_name, series_name, region, observation_time, ingest_time, value, unit, frequency)
		VALUES ('eia_storage_stats', 'storage_5yr_avg_total', 'total', '2026-03-01T00:00:00Z', ?, 1850.0, 'Bcf', 'weekly')
	`, now)
	if err != nil {
		t.Fatalf("seed storage_stats avg: %v", err)
	}

	// facts_time_series — fred spot price
	_, err = db.Exec(`
		INSERT INTO facts_time_series
			(source_name, series_name, region, observation_time, ingest_time, value, unit, frequency)
		VALUES ('fred', 'ng_spot_price', 'US', '2026-03-05T00:00:00Z', ?, 4.25, 'USD/MMBtu', 'daily')
	`, now)
	if err != nil {
		t.Fatalf("seed fred spot: %v", err)
	}

	// facts_time_series — CFTC COT positions
	cotRows := []struct {
		series string
		value  float64
	}{
		{"cot_mm_long", 120000},
		{"cot_mm_short", 80000},
		{"cot_open_interest", 400000},
	}
	for _, c := range cotRows {
		_, err := db.Exec(`
			INSERT INTO facts_time_series
				(source_name, series_name, region, observation_time, ingest_time, value, unit, frequency)
			VALUES ('cftc', ?, 'US', '2026-03-04T00:00:00Z', ?, ?, 'contracts', 'weekly')
		`, c.series, now, c.value)
		if err != nil {
			t.Fatalf("seed cot %s: %v", c.series, err)
		}
	}

	// facts_time_series — EIA supply
	supplyRows := []struct {
		series string
		value  float64
		unit   string
	}{
		{"dry_gas_production_mmcf", 104500, "MMcf"},
		{"lng_exports_mmcf", 14200, "MMcf"},
	}
	for _, s := range supplyRows {
		_, err := db.Exec(`
			INSERT INTO facts_time_series
				(source_name, series_name, region, observation_time, ingest_time, value, unit, frequency)
			VALUES ('eia_supply', ?, 'US', '2026-02-01T00:00:00Z', ?, ?, ?, 'monthly')
		`, s.series, now, s.value, s.unit)
		if err != nil {
			t.Fatalf("seed supply %s: %v", s.series, err)
		}
	}

	// collector_health
	_, err = db.Exec(`
		INSERT INTO collector_health
			(source_name, last_attempt, last_success, last_status, consecutive_failures)
		VALUES ('eia_storage', ?, ?, 'ok', 0)
	`, now, now)
	if err != nil {
		t.Fatalf("seed collector_health: %v", err)
	}
}

// newTestServer builds a Handler from the seeded DB and returns an httptest server.
func newTestServer(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	db, cleanup := newTestDB(t)
	h := &handler.Handler{
		DB:          db,
		Broker:      sse.NewBroker(),
		InternalKey: "test-key",
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/score",        h.Score)
	mux.HandleFunc("GET /api/storage",      h.Storage)
	mux.HandleFunc("GET /api/price",        h.Price)
	mux.HandleFunc("GET /api/weather",      h.Weather)
	mux.HandleFunc("GET /api/supply",       h.Supply)
	mux.HandleFunc("GET /api/cot",          h.COT)
	mux.HandleFunc("GET /api/health",       h.Health)
	mux.HandleFunc("GET /api/stream",       h.Stream)
	mux.HandleFunc("POST /internal/notify", h.Notify)

	srv := httptest.NewServer(mux)
	return srv, func() {
		srv.Close()
		cleanup()
	}
}

// get performs a GET request against the test server and returns the decoded JSON body.
func get(t *testing.T, srv *httptest.Server, path string) map[string]any {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: expected 200, got %d body=%s", path, resp.StatusCode, body)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("GET %s: decode JSON: %v", path, err)
	}
	return out
}

// --- Tests ---

func TestScore_ReturnsScoreAndDrivers(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	body := get(t, srv, "/api/score")

	if body["score"] == nil {
		t.Fatal("expected score field in response")
	}
	score, ok := body["score"].(float64)
	if !ok {
		t.Fatalf("score is not a number: %T", body["score"])
	}
	if score != 11.4 {
		t.Errorf("expected score 11.4, got %v", score)
	}
	if body["label"] != "Mildly Bullish" {
		t.Errorf("expected label 'Mildly Bullish', got %v", body["label"])
	}
	drivers, ok := body["drivers"].([]any)
	if !ok || len(drivers) == 0 {
		t.Error("expected non-empty drivers array")
	}
}

func TestScore_WhatChangedIncluded(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	body := get(t, srv, "/api/score")
	wc, ok := body["what_changed"].([]any)
	if !ok || len(wc) == 0 {
		t.Fatal("expected non-empty what_changed array")
	}
	first, ok := wc[0].(map[string]any)
	if !ok {
		t.Fatal("what_changed[0] is not an object")
	}
	if first["feature"] != "storage_deficit_vs_5yr_bcf" {
		t.Errorf("unexpected first feature: %v", first["feature"])
	}
}

func TestStorage_ReturnsFeaturesAndBand(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	body := get(t, srv, "/api/storage")

	features, ok := body["features"].([]any)
	if !ok || len(features) == 0 {
		t.Fatal("expected non-empty features array")
	}

	// Verify storage_total_bcf is present with the seeded value.
	found := false
	for _, f := range features {
		fm := f.(map[string]any)
		if fm["name"] == "storage_total_bcf" {
			found = true
			if fm["value"].(float64) != 1755.0 {
				t.Errorf("expected storage_total_bcf = 1755.0, got %v", fm["value"])
			}
		}
	}
	if !found {
		t.Error("storage_total_bcf not found in features")
	}

	band, ok := body["five_year_band"].(map[string]any)
	if !ok || band == nil {
		t.Fatal("expected five_year_band in response")
	}
	if band["avg_bcf"].(float64) != 1850.0 {
		t.Errorf("expected avg_bcf = 1850.0, got %v", band["avg_bcf"])
	}

	if body["latest_week_ending"] == nil {
		t.Error("expected latest_week_ending field")
	}
}

func TestPrice_ReturnsFredSpot(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	body := get(t, srv, "/api/price")

	spot, ok := body["spot_history"].([]any)
	if !ok || len(spot) == 0 {
		t.Fatal("expected non-empty spot_history")
	}
	first := spot[0].(map[string]any)
	if first["price"].(float64) != 4.25 {
		t.Errorf("expected FRED spot price 4.25, got %v", first["price"])
	}
}

func TestSupply_ReturnsMostRecentValues(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	body := get(t, srv, "/api/supply")

	series, ok := body["series"].([]any)
	if !ok || len(series) == 0 {
		t.Fatal("expected non-empty series array")
	}

	found := false
	for _, s := range series {
		sm := s.(map[string]any)
		if sm["name"] == "dry_gas_production_mmcf" {
			found = true
			if sm["value"].(float64) != 104500 {
				t.Errorf("expected dry_gas_production_mmcf = 104500, got %v", sm["value"])
			}
		}
	}
	if !found {
		t.Error("dry_gas_production_mmcf not found in supply response")
	}
}

func TestCOT_ReturnsHistoryWithMMNet(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	body := get(t, srv, "/api/cot")

	history, ok := body["history"].([]any)
	if !ok || len(history) == 0 {
		t.Fatal("expected non-empty history array")
	}
	first := history[0].(map[string]any)
	// mm_net = 120000 - 80000 = 40000
	if first["mm_net"].(float64) != 40000 {
		t.Errorf("expected mm_net = 40000, got %v", first["mm_net"])
	}
	// mm_net_pct_oi = 40000 / 400000 * 100 = 10.0
	if first["mm_net_pct_oi"].(float64) != 10.0 {
		t.Errorf("expected mm_net_pct_oi = 10.0, got %v", first["mm_net_pct_oi"])
	}
}

func TestHealth_ReturnsCollectors(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	body := get(t, srv, "/api/health")

	collectors, ok := body["collectors"].([]any)
	if !ok || len(collectors) == 0 {
		t.Fatal("expected non-empty collectors array")
	}
	first := collectors[0].(map[string]any)
	if first["source_name"] != "eia_storage" {
		t.Errorf("expected source_name 'eia_storage', got %v", first["source_name"])
	}
	if first["last_status"] != "ok" {
		t.Errorf("expected last_status 'ok', got %v", first["last_status"])
	}
	if body["server_time"] == nil {
		t.Error("expected server_time field")
	}
}

func TestNotify_RequiresKey(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	// Missing key — must return 401.
	resp, err := http.Post(srv.URL+"/internal/notify", "text/plain", strings.NewReader("eia_storage"))
	if err != nil {
		t.Fatalf("POST /internal/notify: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 without key, got %d", resp.StatusCode)
	}
}

func TestNotify_WithValidKey_Returns204(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/internal/notify", strings.NewReader("eia_storage"))
	req.Header.Set("X-Internal-Key", "test-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /internal/notify: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204, got %d", resp.StatusCode)
	}
}

func TestSSE_ReceivesNotifyEvent(t *testing.T) {
	srv, cleanup := newTestServer(t)
	defer cleanup()

	// Subscribe to the SSE stream in a goroutine.
	// Accumulate bytes across multiple reads: the first read returns the
	// keep-alive comment (": connected\n\n"), and the event arrives in a
	// subsequent read after /internal/notify is called.
	eventCh := make(chan string, 1)
	go func() {
		resp, err := http.Get(srv.URL + "/api/stream")
		if err != nil {
			return
		}
		defer resp.Body.Close()
		buf := make([]byte, 512)
		var accumulated string
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				accumulated += string(buf[:n])
				if strings.Contains(accumulated, "collection_complete") {
					eventCh <- accumulated
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	// Give the subscriber goroutine time to connect.
	time.Sleep(50 * time.Millisecond)

	// Fire a notify with the correct key.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/internal/notify", strings.NewReader("eia_storage"))
	req.Header.Set("X-Internal-Key", "test-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("notify POST: %v", err)
	}
	resp.Body.Close()

	select {
	case msg := <-eventCh:
		if !strings.Contains(msg, "collection_complete") {
			t.Errorf("expected 'collection_complete' event, got: %q", msg)
		}
	case <-time.After(2 * time.Second):
		t.Error("timed out waiting for SSE event")
	}
}
