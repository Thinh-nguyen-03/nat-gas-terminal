// Package sse implements a simple in-memory Server-Sent Events fan-out broker.
//
// When the Python scheduler completes a collection run it POSTs to
// /internal/notify. The broker fans that event out to all connected browser
// clients on /api/stream. Each client then re-fetches the relevant panel
// endpoint to get fresh data.
package sse

import (
	"fmt"
	"net/http"
	"sync"
)

// Broker holds the set of active SSE subscribers and fans out messages to them.
type Broker struct {
	mu          sync.Mutex
	subscribers map[chan string]struct{}
}

// NewBroker creates a ready-to-use Broker.
func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[chan string]struct{}),
	}
}

// Publish sends an SSE event with the given name and data to all connected clients.
// Slow clients whose channels are full are skipped to avoid blocking the notifier.
func (b *Broker) Publish(event, data string) {
	msg := fmt.Sprintf("event: %s\ndata: %s\n\n", event, data)
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subscribers {
		select {
		case ch <- msg:
		default:
			// Client is not reading fast enough — skip rather than block.
		}
	}
}

// ServeHTTP implements http.Handler. It streams SSE events to the client until
// the request context is cancelled (browser tab closed / navigation).
func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Prevent Nginx / proxies from buffering the stream.
	w.Header().Set("X-Accel-Buffering", "no")

	ch := make(chan string, 8)

	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()

	defer func() {
		b.mu.Lock()
		delete(b.subscribers, ch)
		b.mu.Unlock()
	}()

	// Send an initial keep-alive comment so the browser knows the connection is open.
	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	for {
		select {
		case msg := <-ch:
			fmt.Fprint(w, msg)
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}
