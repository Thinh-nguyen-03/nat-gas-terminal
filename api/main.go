// Command api is the read-only HTTP API server for the Natural Gas Intelligence Terminal.
//
// It opens the shared DuckDB file in READ_ONLY mode and serves JSON panel data
// to the Next.js frontend. It also acts as an SSE broker: the Python scheduler
// POSTs to /internal/notify after each collection run, and connected browser
// clients receive a "collection_complete" event and re-fetch their panel.
//
// Usage:
//
//	DB_PATH=../data/db/terminal.duckdb \
//	PORT=8080 \
//	INTERNAL_API_KEY=<your-key> \
//	ALLOWED_ORIGIN=http://localhost:3000 \
//	go run ./main.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	appdb "github.com/nat-gas-terminal/api/internal/db"
	"github.com/nat-gas-terminal/api/internal/handler"
	"github.com/nat-gas-terminal/api/internal/sse"
)

func main() {
	// Structured JSON logging to stdout.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	dbPath := env("DB_PATH", "../data/db/terminal.duckdb")
	port := env("PORT", "8080")
	internalKey := env("INTERNAL_API_KEY", "")
	allowedOrigin := env("ALLOWED_ORIGIN", "http://localhost:3000")

	db, err := appdb.Open(dbPath)
	if err != nil {
		slog.Error("failed to open database", "path", dbPath, "err", err)
		os.Exit(1)
	}
	defer db.Close()

	broker := sse.NewBroker()
	h := &handler.Handler{
		DB:          db,
		Broker:      broker,
		InternalKey: internalKey,
	}

	mux := http.NewServeMux()

	// Public panel endpoints consumed by the Next.js frontend.
	mux.HandleFunc("GET /api/score",   h.Score)
	mux.HandleFunc("GET /api/storage", h.Storage)
	mux.HandleFunc("GET /api/price",   h.Price)
	mux.HandleFunc("GET /api/weather", h.Weather)
	mux.HandleFunc("GET /api/supply",  h.Supply)
	mux.HandleFunc("GET /api/cot",     h.COT)
	mux.HandleFunc("GET /api/health",  h.Health)
	mux.HandleFunc("GET /api/stream",  h.Stream)

	// New panel endpoints (Features 1–7).
	// /api/calendar — live from Day 1 (collectors/catalyst_calendar.py)
	// /api/analogs  — stub; populates when transforms/features_analog.py runs (Feature 6)
	// /api/balance  — partial now (EIA supply + HDD demand); full with Feature 1 EBB data
	// /api/lng      — stub; populates when collectors/lng_vessels.py runs (Feature 2)
	// /api/power    — stub; populates when collectors/iso_lmp.py runs (Feature 3)
	mux.HandleFunc("GET /api/calendar", h.Calendar)
	mux.HandleFunc("GET /api/analogs",  h.Analogs)
	mux.HandleFunc("GET /api/balance",  h.Balance)
	mux.HandleFunc("GET /api/lng",      h.LNG)
	mux.HandleFunc("GET /api/power",    h.Power)

	// Internal endpoint called by the Python scheduler — not exposed publicly.
	mux.HandleFunc("POST /internal/notify", h.Notify)

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: recoverPanic(cors(allowedOrigin, limitMiddleware(mux))),
		// 30s write timeout for all handlers. The SSE handler clears its own
		// per-response deadline via http.NewResponseController so it is unaffected.
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start server in a goroutine so the main goroutine can wait for signals.
	go func() {
		slog.Info("server starting", "addr", srv.Addr, "db", dbPath)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	// Block until SIGINT or SIGTERM.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("shutdown error", "err", err)
	}
}

// limitMiddleware applies two rate limits:
//   - A global limit on POST /internal/notify to prevent SSE spam from a runaway
//     scheduler: 2 events/s sustained, burst of 10 (covers all 21 collectors firing
//     simultaneously on startup).
//   - A per-IP limit on all routes to prevent DB hammering: 1 req/s sustained,
//     burst of 30 (covers a dashboard loading ~10 panels in parallel). SSE connections
//     consume one token on connect only.
//
// Stale IP entries are evicted every 5 minutes to bound memory growth.
func limitMiddleware(next http.Handler) http.Handler {
	notifyLim := rate.NewLimiter(2, 10)

	type clientEntry struct {
		lim      *rate.Limiter
		lastSeen time.Time
	}
	var (
		mu      sync.Mutex
		clients = make(map[string]*clientEntry)
	)
	go func() {
		for range time.Tick(5 * time.Minute) {
			mu.Lock()
			for ip, e := range clients {
				if time.Since(e.lastSeen) > 5*time.Minute {
					delete(clients, ip)
				}
			}
			mu.Unlock()
		}
	}()

	getLimiter := func(ip string) *rate.Limiter {
		mu.Lock()
		defer mu.Unlock()
		e, ok := clients[ip]
		if !ok {
			e = &clientEntry{lim: rate.NewLimiter(1, 30)}
			clients[ip] = e
		}
		e.lastSeen = time.Now()
		return e.lim
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/notify" && !notifyLim.Allow() {
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		if !getLimiter(ip).Allow() {
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// cors wraps a handler with permissive CORS headers for the configured origin.
// Only the explicitly configured origin is reflected — wildcard is never used
// so that credentials (cookies/auth headers) remain safe.
func cors(allowedOrigin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Vary tells caches that the response depends on Origin — required
		// when the server conditionally sets Access-Control-Allow-Origin.
		w.Header().Set("Vary", "Origin")

		origin := r.Header.Get("Origin")
		if origin == allowedOrigin {
			w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Internal-Key")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// recoverPanic catches panics in any downstream handler and returns a 500
// instead of crashing the server. The stack trace is logged for debugging.
func recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rv := recover(); rv != nil {
				slog.Error("panic recovered",
					"method", r.Method,
					"path", r.URL.Path,
					"panic", fmt.Sprint(rv),
					"stack", string(debug.Stack()),
				)
				// Only write if headers haven't been flushed yet.
				if !strings.Contains(w.Header().Get("Content-Type"), "text/event-stream") {
					http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
				}
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// env returns the value of the named environment variable, or defaultVal if unset.
func env(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
