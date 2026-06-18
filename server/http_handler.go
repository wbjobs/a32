package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"
)

type HTTPHandler struct {
	heatMap    *HeatMap
	storage    *Storage
	calculator *BaselineCalculator
	detector   *AnomalyDetector
	mux        *http.ServeMux
}

func NewHTTPHandler(heatMap *HeatMap, storage *Storage, calculator *BaselineCalculator, detector *AnomalyDetector) *HTTPHandler {
	h := &HTTPHandler{
		heatMap:    heatMap,
		storage:    storage,
		calculator: calculator,
		detector:   detector,
		mux:        http.NewServeMux(),
	}
	h.mux.HandleFunc("/api/v1/heat/top", h.handleTopN)
	h.mux.HandleFunc("/api/v1/table/queries", h.handleTableQueries)
	h.mux.HandleFunc("/api/v1/table/baseline", h.handleTableBaseline)
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
		Tables:           make([]TableHeatJSON, 0, len(stats)),
		StartTime:        start.Format(time.RFC3339),
		EndTime:          end.Format(time.RFC3339),
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

type TableQueryDetail struct {
	UUID         string  `json:"uuid"`
	AgentID      string  `json:"agent_id"`
	SQL          string  `json:"sql"`
	Database     string  `json:"database"`
	ClientIP     string  `json:"client_ip"`
	ThreadID     int64   `json:"thread_id"`
	QueryTime    float64 `json:"query_time"`
	LockTime     float64 `json:"lock_time"`
	RowsSent     int64   `json:"rows_sent"`
	RowsExamined int64   `json:"rows_examined"`
	Timestamp    string  `json:"timestamp"`
	SQLType      string  `json:"sql_type"`
}

type TableQueriesResponse struct {
	TableName string              `json:"table_name"`
	StartTime string              `json:"start_time"`
	EndTime   string              `json:"end_time"`
	Total     int                 `json:"total"`
	Queries   []TableQueryDetail  `json:"queries"`
}

func (h *HTTPHandler) handleTableQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	tableName := r.URL.Query().Get("table")
	if tableName == "" {
		http.Error(w, `{"error":"table parameter is required"}`, http.StatusBadRequest)
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	if startStr == "" {
		startStr = time.Now().Add(-1 * time.Hour).Format(time.RFC3339)
	}
	if endStr == "" {
		endStr = time.Now().Format(time.RFC3339)
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

	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 1000 {
			limit = n
		}
	}

	if h.storage == nil {
		http.Error(w, `{"error":"storage not available"}`, http.StatusServiceUnavailable)
		return
	}

	ctx := r.Context()
	records, err := h.storage.GetSlowQueriesByTable(ctx, tableName, start, end, limit)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"query failed: %v"}`, err), http.StatusInternalServerError)
		return
	}

	queries := make([]TableQueryDetail, 0, len(records))
	for _, rec := range records {
		queries = append(queries, TableQueryDetail{
			UUID:         rec.UUID,
			AgentID:      rec.AgentID,
			SQL:          rec.SQL,
			Database:     rec.Database,
			ClientIP:     rec.ClientIP,
			ThreadID:     rec.ThreadID,
			QueryTime:    rec.QueryTime,
			LockTime:     rec.LockTime,
			RowsSent:     rec.RowsSent,
			RowsExamined: rec.RowsExamined,
			Timestamp:    rec.Timestamp.Format(time.RFC3339),
			SQLType:      rec.SQLType,
		})
	}

	response := TableQueriesResponse{
		TableName: tableName,
		StartTime: start.Format(time.RFC3339),
		EndTime:   end.Format(time.RFC3339),
		Total:     len(queries),
		Queries:   queries,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

type TableBaselineResponse struct {
	TableName     string  `json:"table_name"`
	MeanPerMinute float64 `json:"mean_per_minute"`
	StdPerMinute  float64 `json:"std_per_minute"`
	SampleCount   int     `json:"sample_count"`
	Sigma3        float64 `json:"sigma_3_threshold"`
	HistoryDays   int     `json:"history_days"`
}

func (h *HTTPHandler) handleTableBaseline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	tableName := r.URL.Query().Get("table")
	if tableName == "" {
		http.Error(w, `{"error":"table parameter is required"}`, http.StatusBadRequest)
		return
	}

	if h.calculator == nil {
		http.Error(w, `{"error":"baseline calculator not available"}`, http.StatusServiceUnavailable)
		return
	}

	baseline, ok := h.calculator.GetBaseline(tableName)
	if !ok {
		http.Error(w, `{"error":"no baseline data for this table"}`, http.StatusNotFound)
		return
	}

	response := TableBaselineResponse{
		TableName:     baseline.TableName,
		MeanPerMinute: roundTo2(baseline.MeanPerMinute),
		StdPerMinute:  roundTo2(baseline.StdPerMinute),
		SampleCount:   baseline.SampleCount,
		Sigma3:        roundTo2(baseline.MeanPerMinute + 3*baseline.StdPerMinute),
		HistoryDays:   7,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *HTTPHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

type HeatResponse struct {
	Tables           []TableHeatJSON `json:"tables"`
	StartTime        string          `json:"start_time"`
	EndTime          string          `json:"end_time"`
	TotalSlowQueries int             `json:"total_slow_queries"`
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
