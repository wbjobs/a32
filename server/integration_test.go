package server

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/trae3/slowquery-lineage/pkg/slowquery"
)

func TestEndToEndPipeline(t *testing.T) {
	hm := NewHeatMap(1*time.Hour, 10000)
	svc := NewGRPCService(hm, nil, nil, nil)

	entries := []*pb.SlowLogEntry{
		{AgentId: "agent-1", Timestamp: "2026-06-19T10:00:01Z", QueryTime: 5.23, LockTime: 3.10, Sql: "SELECT u.id, u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'pending'", Database: "myapp"},
		{AgentId: "agent-1", Timestamp: "2026-06-19T10:00:05Z", QueryTime: 12.50, LockTime: 10.20, Sql: "UPDATE products SET price = price * 1.1 WHERE category = 'electronics'", Database: "myapp"},
		{AgentId: "agent-2", Timestamp: "2026-06-19T10:00:10Z", QueryTime: 2.10, LockTime: 0.50, Sql: "SELECT id, token FROM sessions WHERE expired_at < NOW()", Database: "myapp"},
		{AgentId: "agent-2", Timestamp: "2026-06-19T10:00:15Z", QueryTime: 8.00, LockTime: 7.50, Sql: "DELETE FROM cache_entries WHERE stale = 1", Database: "myapp"},
		{AgentId: "agent-1", Timestamp: "2026-06-19T10:00:20Z", QueryTime: 15.00, LockTime: 12.00, Sql: "SELECT * FROM orders WHERE created_at > '2026-01-01' ORDER BY total DESC", Database: "myapp"},
	}

	for _, entry := range entries {
		events := svc.processEntry(entry)
		for _, ev := range events {
			hm.Add(ev)
		}
	}

	start, _ := time.Parse(time.RFC3339, "2026-06-19T09:00:00Z")
	end, _ := time.Parse(time.RFC3339, "2026-06-19T11:00:00Z")

	top := hm.TopN(start, end, 10)
	if len(top) == 0 {
		t.Fatal("expected at least one table in results")
	}

	t.Logf("Heat ranking:")
	for i, s := range top {
		t.Logf("  #%d: %s (score=%.2f, count=%d, qt=%.2f, lt=%.2f, fields=%v)",
			i+1, s.TableName, s.HeatScore, s.SlowQueryCount,
			s.TotalQueryTime, s.TotalLockTime, s.AffectedFields)
	}

	if top[0].TableName != "orders" {
		t.Errorf("expected orders as hottest table (2 slow queries), got %s", top[0].TableName)
	}

	handler := NewHTTPHandler(hm, nil, nil, nil)
	req := httptest.NewRequest("GET", "/api/v1/heat/top?start=2026-06-19T09:00:00Z&end=2026-06-19T11:00:00Z&n=10", nil)
	w := httptest.NewRecorder()
	handler.Handler().ServeHTTP(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, string(body))
	}

	var result HeatResponse
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse JSON: %v", err)
	}

	if len(result.Tables) == 0 {
		t.Fatal("expected tables in HTTP response")
	}
	if result.TotalSlowQueries < 5 {
		t.Errorf("expected at least 5 total slow queries, got %d", result.TotalSlowQueries)
	}
	if result.Tables[0].TableName != "orders" {
		t.Errorf("expected orders as hottest, got %s", result.Tables[0].TableName)
	}

	t.Logf("HTTP response: %s", string(body))

	reqHealth := httptest.NewRequest("GET", "/api/v1/health", nil)
	wHealth := httptest.NewRecorder()
	handler.Handler().ServeHTTP(wHealth, reqHealth)
	if wHealth.Result().StatusCode != 200 {
		t.Error("health check failed")
	}
}
