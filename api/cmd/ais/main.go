// Command ais is a long-running AIS vessel tracker for US LNG export terminals.
//
// Connects permanently to AISstream.io WebSocket, maintains in-memory vessel
// state, and every 5 minutes POSTs terminal berth counts and per-vessel
// snapshots to the API server (POST /internal/ais), which writes them to DuckDB
// and fans out an SSE event to connected browser clients.
//
// Usage:
//
//	AISSTREAM_API_KEY=<key> \
//	API_NOTIFY_URL=http://localhost:8080/internal/ais \
//	INTERNAL_API_KEY=<key> \
//	go run ./cmd/ais
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

const (
	wsURL         = "wss://stream.aisstream.io/v0/stream"
	writeInterval = 5 * time.Minute // how often to flush counts to API server
	reconnectBase = 5 * time.Second // initial reconnect wait
	reconnectMax  = 5 * time.Minute // cap on reconnect backoff
	lngShipType   = 84              // AIS ship type: Liquefied gas tanker
)

type terminalDef struct {
	name string
	lat  float64
	lon  float64
	box  float64 // bounding box half-width in degrees (~11 km per 0.10°)
}

var terminals = []terminalDef{
	{"Sabine Pass", 29.726, -93.872, 0.10},
	{"Corpus Christi", 27.636, -97.325, 0.10},
	{"Freeport LNG", 28.861, -95.315, 0.10},
	{"Cameron LNG", 29.817, -93.292, 0.10},
	{"Calcasieu Pass", 29.809, -93.347, 0.15},
	{"Cove Point", 38.406, -76.540, 0.10},
	{"Elba Island", 32.082, -81.102, 0.10},
}

// Two bounding boxes covering all terminals: Gulf Coast + East Coast.
// AISstream.io format: [[lat_min, lon_min], [lat_max, lon_max]]
var subscriptionBoxes = [2][2][2]float64{
	{{27.0, -98.0}, {30.5, -92.0}},
	{{31.5, -82.0}, {39.0, -75.0}},
}

// AIS message types (AISstream.io JSON schema)

type aisMessage struct {
	MessageType string         `json:"MessageType"`
	MetaData    aisMetadata    `json:"MetaData"`
	Message     aisMessageBody `json:"Message"`
}

type aisMetadata struct {
	MMSI      int     `json:"MMSI"`
	ShipName  string  `json:"ShipName"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type aisMessageBody struct {
	PositionReport *positionReport `json:"PositionReport"`
	ShipStaticData *shipStaticData `json:"ShipStaticData"`
}

type positionReport struct {
	Latitude           float64 `json:"Latitude"`
	Longitude          float64 `json:"Longitude"`
	Sog                float64 `json:"Sog"`
	NavigationalStatus int     `json:"NavigationalStatus"`
}

type shipStaticData struct {
	Type        int     `json:"Type"`
	Name        string  `json:"Name"`
	Destination string  `json:"Destination"`
	Draught     float64 `json:"Draught"`
}

type vessel struct {
	mmsi        int
	name        string
	shipType    int // -1 = unknown
	lat         float64
	lon         float64
	sog         float64
	nav         int // AIS NavigationalStatus; -1 = unknown
	hasPos      bool
	hasType     bool
	destination string
	draught     float64
}

// vesselPayload mirrors the aisVesselPayload struct in the API server.
type vesselPayload struct {
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

// aisUpdateBody is what we POST to /internal/ais.
type aisUpdateBody struct {
	Counts  map[string][2]int `json:"counts"`
	Vessels []vesselPayload   `json:"vessels"`
}

type vesselMap struct {
	mu   sync.RWMutex
	data map[int]*vessel
}

func newVesselMap() *vesselMap { return &vesselMap{data: make(map[int]*vessel)} }

func (vm *vesselMap) update(msg aisMessage) {
	mmsi := msg.MetaData.MMSI
	if mmsi == 0 {
		return
	}

	vm.mu.Lock()
	defer vm.mu.Unlock()

	v, ok := vm.data[mmsi]
	if !ok {
		v = &vessel{mmsi: mmsi, shipType: -1, nav: -1}
		vm.data[mmsi] = v
	}
	if n := strings.TrimSpace(msg.MetaData.ShipName); n != "" {
		v.name = n
	}

	if pr := msg.Message.PositionReport; pr != nil {
		lat := pr.Latitude
		if lat == 0 && msg.MetaData.Latitude != 0 {
			lat = msg.MetaData.Latitude
		}
		lon := pr.Longitude
		if lon == 0 && msg.MetaData.Longitude != 0 {
			lon = msg.MetaData.Longitude
		}
		if lat != 0 || lon != 0 {
			v.lat, v.lon = lat, lon
			v.hasPos = true
		}
		v.sog = pr.Sog
		v.nav = pr.NavigationalStatus
	}

	if sd := msg.Message.ShipStaticData; sd != nil {
		v.shipType = sd.Type
		v.hasType = true
		if n := strings.TrimSpace(sd.Name); n != "" {
			v.name = n
		}
		if d := strings.TrimSpace(sd.Destination); d != "" {
			v.destination = d
		}
		if sd.Draught > 0 {
			v.draught = sd.Draught
		}
	}
}

func (vm *vesselMap) classify() (map[string][2]int, []vesselPayload) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	counts := make(map[string][2]int)
	var vessels []vesselPayload

	for _, v := range vm.data {
		if !v.hasPos {
			continue
		}

		// Type filter: confirmed type-84 LNG tanker, OR provisionally include
		// untyped vessels that are nearly stopped within tight berth range.
		// Provisional vessels are later marked status="provisional" in the payload.
		// Tugs/pilots are excluded because they move constantly (SOG > 0.3 kts)
		// or park outside the tight berth radius.
		isConfirmed := v.hasType && v.shipType == lngShipType

		for _, t := range terminals {
			d := haversineDeg(v.lat, v.lon, t.lat, t.lon)
			if d > t.box {
				continue
			}
			isBerth := d <= 0.05
			isTightBerth := d <= 0.03 // ~3 km — squarely on the dock
			isMoored := v.nav == 5 || (v.sog < 0.5 && isBerth)
			isAnchored := v.nav == 1 || (v.sog < 2.0 && !isMoored && d <= t.box)

			// Provisional: untyped vessel that is nearly stationary at the dock.
			if !isConfirmed && !(isTightBerth && v.sog < 0.3) {
				continue
			}

			c := counts[t.name]
			var status string
			if isMoored || (!isConfirmed && isTightBerth) {
				c[0]++
				if isConfirmed {
					status = "loading"
				} else {
					status = "provisional"
				}
			} else if isAnchored {
				c[1]++
				status = "anchored"
			}
			counts[t.name] = c

			if status != "" {
				vessels = append(vessels, vesselPayload{
					MMSI:        v.mmsi,
					Name:        v.name,
					Terminal:    t.name,
					Status:      status,
					Lat:         v.lat,
					Lon:         v.lon,
					Sog:         v.sog,
					NavStatus:   v.nav,
					Destination: v.destination,
					Draught:     v.draught,
				})
			}
		}
	}
	return counts, vessels
}

func streamLoop(apiKey string, vm *vesselMap, stop <-chan struct{}) {
	backoff := reconnectBase
	for {
		select {
		case <-stop:
			return
		default:
		}

		if err := runSession(apiKey, vm, stop); err != nil {
			slog.Warn("AIS session ended", "err", err, "reconnect_in", backoff)
		} else {
			slog.Info("AIS session closed cleanly — reconnecting")
		}

		select {
		case <-stop:
			return
		case <-time.After(backoff):
		}
		backoff = minDuration(backoff*2, reconnectMax)
	}
}

func runSession(apiKey string, vm *vesselMap, stop <-chan struct{}) error {
	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 0

	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	sub, _ := json.Marshal(map[string]any{
		"APIKey":             apiKey,
		"BoundingBoxes":      subscriptionBoxes,
		"FilterMessageTypes": []string{"PositionReport", "ShipStaticData"},
	})
	if err := conn.WriteMessage(websocket.TextMessage, sub); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	slog.Info("AISstream.io connected")

	msgCh := make(chan []byte, 256)
	errCh := make(chan error, 1)

	go func() {
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			select {
			case msgCh <- raw:
			default:
			}
		}
	}()

	// AISstream.io drops idle connections after ~2 min without a ping.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
					return
				}
			case <-stop:
				return
			}
		}
	}()

	for {
		select {
		case <-stop:
			conn.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return nil
		case err := <-errCh:
			return fmt.Errorf("read: %w", err)
		case raw := <-msgCh:
			var msg aisMessage
			if err := json.Unmarshal(raw, &msg); err == nil {
				vm.update(msg)
			}
		}
	}
}

func writeLoop(apiURL, apiKey string, vm *vesselMap, stop <-chan struct{}) {
	ticker := time.NewTicker(writeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			logVesselDiagnostics(vm)
			counts, vessels := vm.classify()
			if err := postWithRetry(apiURL, apiKey, counts, vessels); err != nil {
				slog.Error("AIS post failed", "err", err)
			}
		}
	}
}

// logVesselDiagnostics logs the vessel map breakdown at each filter stage.
func logVesselDiagnostics(vm *vesselMap) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	total := len(vm.data)
	withPos, withType, type84, inBox := 0, 0, 0, 0
	for _, v := range vm.data {
		if v.hasPos {
			withPos++
		}
		if v.hasType {
			withType++
		}
		if v.hasType && v.shipType == lngShipType {
			type84++
			// check if in any terminal box
			for _, t := range terminals {
				if haversineDeg(v.lat, v.lon, t.lat, t.lon) <= t.box {
					inBox++
					break
				}
			}
		}
	}
	slog.Info("AIS vessel map",
		"total_seen", total,
		"with_position", withPos,
		"with_static_data", withType,
		"type_84_lng", type84,
		"type_84_in_box", inBox,
	)
}

func postWithRetry(apiURL, apiKey string, counts map[string][2]int, vessels []vesselPayload) error {
	const maxAttempts = 4
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := postAIS(apiURL, apiKey, counts, vessels)
		if err == nil {
			return nil
		}
		if attempt < maxAttempts {
			slog.Warn("AIS post failed — retrying", "attempt", attempt, "err", err)
			time.Sleep(time.Duration(attempt*5) * time.Second)
			continue
		}
		return err
	}
	return fmt.Errorf("exhausted retries")
}

func postAIS(apiURL, apiKey string, counts map[string][2]int, vessels []vesselPayload) error {
	body := aisUpdateBody{Counts: counts, Vessels: vessels}
	if body.Vessels == nil {
		body.Vessels = []vesselPayload{}
	}

	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, apiURL, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("X-Internal-Key", apiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	loading, anchored := 0, 0
	for _, c := range counts {
		loading += c[0]
		anchored += c[1]
	}
	slog.Info("AIS update posted", "loading", loading, "anchored", anchored, "vessels", len(vessels))
	return nil
}

func haversineDeg(lat1, lon1, lat2, lon2 float64) float64 {
	dlat := math.Abs(lat1 - lat2)
	dlon := math.Abs(lon1-lon2) * math.Cos(math.Pi/180*((lat1+lat2)/2))
	return math.Sqrt(dlat*dlat + dlon*dlon)
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	// Load ../.env (project root) if present.
	_ = godotenv.Load("../.env")

	apiKey := mustEnv("AISSTREAM_API_KEY")
	apiURL := env("API_NOTIFY_URL", "http://localhost:8080/internal/ais")
	internalKey := env("INTERNAL_API_KEY", "")

	vm := newVesselMap()
	stop := make(chan struct{})

	go streamLoop(apiKey, vm, stop)
	go writeLoop(apiURL, internalKey, vm, stop)

	slog.Info("AIS collector started",
		"terminals", len(terminals),
		"write_interval", writeInterval,
		"api_url", apiURL,
	)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down AIS collector")
	close(stop)
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required env var not set", "key", key)
		os.Exit(1)
	}
	return v
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
