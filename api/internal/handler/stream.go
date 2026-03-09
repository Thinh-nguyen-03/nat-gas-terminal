package handler

import (
	"crypto/subtle"
	"io"
	"log/slog"
	"net/http"
	"strings"
)

// Stream handles GET /api/stream.
// Delegates entirely to the SSE broker which manages subscriber lifecycle.
func (h *Handler) Stream(w http.ResponseWriter, r *http.Request) {
	h.Broker.ServeHTTP(w, r)
}

// Notify handles POST /internal/notify.
// The Python scheduler calls this endpoint after each successful collection run.
// It fans out a "collection_complete" SSE event to all connected browser clients,
// which then re-fetch their respective panel endpoints.
//
// Authentication: if InternalKey is non-empty, the request must carry it in the
// X-Internal-Key header. Requests with a wrong or missing key are rejected with
// 401 to prevent unauthenticated SSE pushes from external callers.
func (h *Handler) Notify(w http.ResponseWriter, r *http.Request) {
	if h.InternalKey != "" {
		got := r.Header.Get("X-Internal-Key")
		if subtle.ConstantTimeCompare([]byte(got), []byte(h.InternalKey)) != 1 {
			writeError(w, http.StatusUnauthorized, "invalid internal key")
			return
		}
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 4096))
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read body")
		return
	}

	// The body is the source_name that just completed (e.g. "eia_storage").
	// Strip newlines before embedding in the SSE frame to prevent protocol injection.
	source := strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' {
			return -1
		}
		return r
	}, strings.TrimSpace(string(body)))
	if source == "" {
		source = "unknown"
	}

	h.Broker.Publish("collection_complete", source)
	slog.Info("notify received", "source", source)

	w.WriteHeader(http.StatusNoContent)
}
