package handler

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// aisUpdateRequest is the JSON body for POST /internal/ais.
type aisUpdateRequest struct {
	Counts  map[string][2]int  `json:"counts"`  // terminal → [loading, anchored]
	Vessels []aisVesselPayload `json:"vessels"`
}

type aisVesselPayload struct {
	MMSI        int     `json:"mmsi"`
	Name        string  `json:"name"`
	Terminal    string  `json:"terminal"`
	Status      string  `json:"status"`
	Lat         float64 `json:"lat"`
	Lon         float64 `json:"lon"`
	Sog         float64 `json:"sog"`
	NavStatus   int     `json:"nav_status"`
	Destination string  `json:"destination"`
	Draught     float64 `json:"draught"`
}

// AISState holds the latest AIS vessel snapshot in memory.
// It is updated by POST /internal/ais and read by GET /api/lng and GET /api/lng/vessels.
// A JSON copy is also written to disk for Python's features_lng transform.
type AISState struct {
	mu        sync.RWMutex
	Counts    map[string][2]int  // terminal → [loading, anchored]
	Vessels   []aisVesselPayload
	firstSeen map[string]time.Time // key: "mmsi:terminal", tracks dwell start
	UpdatedAt time.Time
}

// NewAISState returns an initialised AISState.
func NewAISState() *AISState {
	return &AISState{
		Counts:    map[string][2]int{},
		firstSeen: map[string]time.Time{},
	}
}

// DwellMinutes returns how long the given (mmsi, terminal) has been tracked.
func (s *AISState) DwellMinutes(mmsi int, terminal string) int {
	key := fmt.Sprintf("%d:%s", mmsi, terminal)
	if t, ok := s.firstSeen[key]; ok {
		return int(time.Since(t).Minutes())
	}
	return 0
}

// AISUpdate handles POST /internal/ais.
// Called by cmd/ais (pure Go) to push vessel counts and snapshots.
// Data is stored in memory only — the API server stays READ_ONLY on DuckDB
// so the Python scheduler can write concurrently.
func (h *Handler) AISUpdate(w http.ResponseWriter, r *http.Request) {
	if h.InternalKey != "" {
		got := r.Header.Get("X-Internal-Key")
		if subtle.ConstantTimeCompare([]byte(got), []byte(h.InternalKey)) != 1 {
			writeError(w, http.StatusUnauthorized, "invalid internal key")
			return
		}
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read body")
		return
	}

	var req aisUpdateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	now := time.Now().UTC()

	h.AIS.mu.Lock()
	// Preserve firstSeen for vessels that are still present.
	newFirstSeen := make(map[string]time.Time, len(req.Vessels))
	for _, v := range req.Vessels {
		key := fmt.Sprintf("%d:%s", v.MMSI, v.Terminal)
		if t, ok := h.AIS.firstSeen[key]; ok {
			newFirstSeen[key] = t
		} else {
			newFirstSeen[key] = now
		}
	}
	h.AIS.Counts = req.Counts
	h.AIS.Vessels = req.Vessels
	h.AIS.firstSeen = newFirstSeen
	h.AIS.UpdatedAt = now
	h.AIS.mu.Unlock()

	if err := h.writeAISSnapshot(req, now); err != nil {
		slog.Warn("AIS snapshot write failed", "err", err)
	}

	h.Broker.Publish("collection_complete", "lng_vessels")
	slog.Info("AIS update stored", "terminals", len(req.Counts), "vessels", len(req.Vessels))
	w.WriteHeader(http.StatusNoContent)
}

// writeAISSnapshot persists the latest AIS data as JSON alongside the DuckDB file.
// Python's features_lng transform reads this file to compute EPI and destination mix.
func (h *Handler) writeAISSnapshot(req aisUpdateRequest, now time.Time) error {
	if h.SnapshotDir == "" {
		return nil
	}
	type snapshot struct {
		Counts    map[string][2]int  `json:"counts"`
		Vessels   []aisVesselPayload `json:"vessels"`
		UpdatedAt string             `json:"updated_at"`
	}
	b, err := json.Marshal(snapshot{
		Counts:    req.Counts,
		Vessels:   req.Vessels,
		UpdatedAt: now.Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	// Write to data/ais_snapshot.json (one directory up from data/db/).
	path := filepath.Join(h.SnapshotDir, "ais_snapshot.json")
	return os.WriteFile(path, b, 0o644)
}
