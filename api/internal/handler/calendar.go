package handler

import (
	"database/sql"
	"log/slog"
	"net/http"
	"time"
)

// CalendarEvent is one entry from the catalyst_calendar table.
type CalendarEvent struct {
	ID          string  `json:"id"`
	EventDate   string  `json:"event_date"`
	EventTimeET *string `json:"event_time_et"`
	EventType   string  `json:"event_type"`
	Description string  `json:"description"`
	Impact      *string `json:"impact"`
	DaysUntil   int     `json:"days_until"`
	IsAuto      bool    `json:"is_auto"`
	Notes       *string `json:"notes"`
}

// CalendarResponse is the JSON body returned by GET /api/calendar.
type CalendarResponse struct {
	Events []CalendarEvent `json:"events"`
	AsOf   string          `json:"as_of"`
}

// Calendar handles GET /api/calendar.
// Returns all catalyst calendar events for the next 30 days, ordered by date.
// Events are populated by collectors/catalyst_calendar.py (runs daily 6 AM ET).
func (h *Handler) Calendar(w http.ResponseWriter, r *http.Request) {
	db := h.DB

	rows, err := db.QueryContext(r.Context(), `
		SELECT
		    id,
		    event_date::VARCHAR,
		    event_time_et,
		    event_type,
		    description,
		    impact,
		    is_auto,
		    notes,
		    (event_date - CURRENT_DATE)::INTEGER AS days_until
		FROM catalyst_calendar
		WHERE event_date >= CURRENT_DATE
		  AND event_date <= CURRENT_DATE + INTERVAL 30 DAYS
		ORDER BY event_date, event_time_et NULLS LAST
	`)
	if err != nil {
		slog.Error("calendar query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer rows.Close()

	var events []CalendarEvent
	for rows.Next() {
		var e CalendarEvent
		var timeET, impact, notes sql.NullString
		if err := rows.Scan(
			&e.ID, &e.EventDate, &timeET, &e.EventType,
			&e.Description, &impact, &e.IsAuto, &notes, &e.DaysUntil,
		); err != nil {
			slog.Warn("calendar row scan failed", "err", err)
			continue
		}
		e.EventTimeET = nullString(timeET)
		e.Impact = nullString(impact)
		e.Notes = nullString(notes)
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	if events == nil {
		events = []CalendarEvent{}
	}

	writeJSON(w, http.StatusOK, CalendarResponse{
		Events: events,
		AsOf:   time.Now().UTC().Format(time.RFC3339),
	})
}
