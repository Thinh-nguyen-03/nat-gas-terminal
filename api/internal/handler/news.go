package handler

import (
	"database/sql"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// NewsItem is one scored headline from the news_items table.
type NewsItem struct {
	ID          string   `json:"id"`
	Source      string   `json:"source"`
	Title       string   `json:"title"`
	URL         *string  `json:"url"`
	PublishedAt *string  `json:"published_at"`
	Score       float64  `json:"score"`
	Sentiment   string   `json:"sentiment"`
	Tags        []string `json:"tags"`
	Implication *string  `json:"implication"`
}

// NewsResponse is the JSON body for GET /api/news.
type NewsResponse struct {
	Items []NewsItem `json:"items"`
	AsOf  string     `json:"as_of"`
}

// News handles GET /api/news.
// Returns the last 48h of scored headlines, ordered by relevance then recency.
func (h *Handler) News(w http.ResponseWriter, r *http.Request) {
	db, err := h.openDB()
	if err != nil {
		slog.Error("db open failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer db.Close()

	rows, err := db.QueryContext(r.Context(), `
		SELECT id, source, title, url, published_at::VARCHAR, score, sentiment, tags, implication
		FROM news_items
		WHERE fetched_at >= NOW()::TIMESTAMP - INTERVAL '7 days'
		ORDER BY published_at DESC NULLS LAST
		LIMIT 30
	`)
	if err != nil {
		slog.Error("news query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	defer rows.Close()

	var items []NewsItem
	for rows.Next() {
		var item NewsItem
		var url, pubAt, tagsRaw, implication sql.NullString
		if err := rows.Scan(
			&item.ID, &item.Source, &item.Title,
			&url, &pubAt, &item.Score, &item.Sentiment, &tagsRaw, &implication,
		); err != nil {
			slog.Warn("news row scan failed", "err", err)
			continue
		}
		item.URL = nullString(url)
		item.PublishedAt = nullString(pubAt)
		item.Implication = nullString(implication)
		if tagsRaw.Valid && tagsRaw.String != "" {
			item.Tags = strings.Split(tagsRaw.String, ",")
		} else {
			item.Tags = []string{}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	if items == nil {
		items = []NewsItem{}
	}
	writeJSON(w, http.StatusOK, NewsResponse{
		Items: items,
		AsOf:  time.Now().UTC().Format(time.RFC3339),
	})
}
