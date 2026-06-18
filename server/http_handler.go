package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"
)

type HTTPHandler struct {
	heatMap *HeatMap
	mux     *http.ServeMux
}

func NewHTTPHandler(heatMap *HeatMap) *HTTPHandler {
	h := &HTTPHandler{
		heatMap: heatMap,
		mux:     http.NewServeMux(),
	}
	h.mux.HandleFunc("/api/v1/heat/top", h.handleTopN)
	h.mux.HandleFunc("/api/v1/health", h.handleHealth)
	return h
}

func (h *HTTPHandler) Handler() http.Handler {
	return h.mux
}

func (h *HTTPHandler) handleTopN(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	if startStr == "" || endStr == "" {
		http.Error(w, `{"error":"start and end parameters are required (RFC3339 format)"}`, http.StatusBadRequest)
		return
	}

	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid start time: %v"}`, err), http.StatusBadRequest)
		return
	}

	end, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid end time: %v"}`, err), http.StatusBadRequest)
		return
	}

	topN := 10
	if n := r.URL.Query().Get("n"); n != "" {
		var parsed int
		if _, err := fmt.Sscanf(n, "%d", &parsed); err == nil && parsed > 0 && parsed <= 100 {
			topN = parsed
		}
	}

	stats := h.heatMap.TopN(start, end, topN)
	total := h.heatMap.TotalInWindow(start, end)

	response := HeatResponse{
		Tables:          make([]TableHeatJSON, 0, len(stats)),
		StartTime:       start.Format(time.RFC3339),
		EndTime:         end.Format(time.RFC3339),
		TotalSlowQueries: total,
	}

	for _, s := range stats {
		fields := sortedFieldKeys(s.AffectedFields)
		response.Tables = append(response.Tables, TableHeatJSON{
			TableName:      s.TableName,
			HeatScore:      roundTo2(s.HeatScore),
			SlowQueryCount: s.SlowQueryCount,
			TotalQueryTime: roundTo2(s.TotalQueryTime),
			TotalLockTime:  roundTo2(s.TotalLockTime),
			AffectedFields: fields,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *HTTPHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

type HeatResponse struct {
	Tables          []TableHeatJSON `json:"tables"`
	StartTime       string          `json:"start_time"`
	EndTime         string          `json:"end_time"`
	TotalSlowQueries int            `json:"total_slow_queries"`
}

type TableHeatJSON struct {
	TableName      string   `json:"table_name"`
	HeatScore      float64  `json:"heat_score"`
	SlowQueryCount int64    `json:"slow_query_count"`
	TotalQueryTime float64  `json:"total_query_time"`
	TotalLockTime  float64  `json:"total_lock_time"`
	AffectedFields []string `json:"affected_fields"`
}

func sortedFieldKeys(m map[string]int) []string {
	type kv struct {
		Key   string
		Value int
	}
	var s []kv
	for k, v := range m {
		s = append(s, kv{k, v})
	}
	sort.Slice(s, func(i, j int) bool {
		return s[i].Value > s[j].Value
	})
	result := make([]string, 0, len(s))
	for _, kv := range s {
		result = append(result, kv.Key)
	}
	return result
}

func roundTo2(f float64) float64 {
	return float64(int(f*100+0.5)) / 100
}
