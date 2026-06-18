package server

import (
	"testing"
	"time"
)

func TestExtractLineage_Select(t *testing.T) {
	result := ExtractLineage("SELECT id, name, email FROM users WHERE status = 1")
	if result.SQLType != "SELECT" {
		t.Errorf("expected SELECT, got %s", result.SQLType)
	}
	if len(result.Tables) != 1 || result.Tables[0] != "users" {
		t.Errorf("expected [users], got %v", result.Tables)
	}
	if len(result.Fields) < 3 {
		t.Errorf("expected at least 3 fields, got %v", result.Fields)
	}
}

func TestExtractLineage_SelectJoin(t *testing.T) {
	result := ExtractLineage("SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id")
	if result.SQLType != "SELECT" {
		t.Errorf("expected SELECT, got %s", result.SQLType)
	}
	if len(result.Tables) != 2 {
		t.Errorf("expected 2 tables, got %v", result.Tables)
	}
	foundUsers := false
	foundOrders := false
	for _, tbl := range result.Tables {
		if tbl == "users" {
			foundUsers = true
		}
		if tbl == "orders" {
			foundOrders = true
		}
	}
	if !foundUsers || !foundOrders {
		t.Errorf("expected users and orders, got %v", result.Tables)
	}
}

func TestExtractLineage_Insert(t *testing.T) {
	result := ExtractLineage("INSERT INTO products (name, price, category) VALUES ('widget', 9.99, 'tools')")
	if result.SQLType != "INSERT" {
		t.Errorf("expected INSERT, got %s", result.SQLType)
	}
	if len(result.Tables) != 1 || result.Tables[0] != "products" {
		t.Errorf("expected [products], got %v", result.Tables)
	}
	if len(result.Fields) < 3 {
		t.Errorf("expected at least 3 fields, got %v", result.Fields)
	}
}

func TestExtractLineage_Update(t *testing.T) {
	result := ExtractLineage("UPDATE accounts SET balance = balance - 100, updated_at = NOW() WHERE id = 42")
	if result.SQLType != "UPDATE" {
		t.Errorf("expected UPDATE, got %s", result.SQLType)
	}
	if len(result.Tables) != 1 || result.Tables[0] != "accounts" {
		t.Errorf("expected [accounts], got %v", result.Tables)
	}
}

func TestExtractLineage_Delete(t *testing.T) {
	result := ExtractLineage("DELETE FROM sessions WHERE expired_at < NOW()")
	if result.SQLType != "DELETE" {
		t.Errorf("expected DELETE, got %s", result.SQLType)
	}
	if len(result.Tables) != 1 || result.Tables[0] != "sessions" {
		t.Errorf("expected [sessions], got %v", result.Tables)
	}
}

func TestExtractLineage_SchemaQualified(t *testing.T) {
	result := ExtractLineage("SELECT id FROM prod_db.users")
	if len(result.Tables) != 1 || result.Tables[0] != "prod_db.users" {
		t.Errorf("expected [prod_db.users], got %v", result.Tables)
	}
}

func TestExtractLineage_BacktickQuoted(t *testing.T) {
	result := ExtractLineage("SELECT `id`, `name` FROM `order_items`")
	if len(result.Tables) != 1 || result.Tables[0] != "order_items" {
		t.Errorf("expected [order_items], got %v", result.Tables)
	}
}

func TestHeatMap_TopN(t *testing.T) {
	hm := NewHeatMap(1*time.Hour, 1000)
	now := time.Now().UTC()

	events := []HeatEvent{
		{TableName: "orders", QueryTime: 5.0, LockTime: 3.0, Timestamp: now, Fields: []string{"id", "status"}, SQLType: "SELECT"},
		{TableName: "orders", QueryTime: 8.0, LockTime: 6.0, Timestamp: now, Fields: []string{"total"}, SQLType: "UPDATE"},
		{TableName: "users", QueryTime: 2.0, LockTime: 0.5, Timestamp: now, Fields: []string{"id"}, SQLType: "SELECT"},
		{TableName: "products", QueryTime: 12.0, LockTime: 10.0, Timestamp: now, Fields: []string{"price"}, SQLType: "UPDATE"},
		{TableName: "sessions", QueryTime: 1.0, LockTime: 0.1, Timestamp: now, Fields: []string{"token"}, SQLType: "DELETE"},
	}

	for _, e := range events {
		hm.Add(e)
	}

	start := now.Add(-1 * time.Hour)
	end := now.Add(1 * time.Hour)

	top := hm.TopN(start, end, 3)
	if len(top) != 3 {
		t.Fatalf("expected 3 results, got %d", len(top))
	}

	if top[0].TableName != "products" {
		t.Errorf("expected products as hottest, got %s (score=%.2f)", top[0].TableName, top[0].HeatScore)
	}
	if top[1].TableName != "orders" {
		t.Errorf("expected orders as second, got %s (score=%.2f)", top[1].TableName, top[1].HeatScore)
	}

	for _, s := range top {
		if s.SlowQueryCount <= 0 {
			t.Errorf("expected positive count for %s, got %d", s.TableName, s.SlowQueryCount)
		}
		if s.HeatScore <= 0 {
			t.Errorf("expected positive heat score for %s, got %.2f", s.TableName, s.HeatScore)
		}
	}
}

func TestHeatMap_TimeWindow(t *testing.T) {
	hm := NewHeatMap(1*time.Hour, 1000)
	now := time.Now().UTC()

	hm.Add(HeatEvent{TableName: "old_table", QueryTime: 5.0, LockTime: 2.0, Timestamp: now.Add(-2 * time.Hour)})
	hm.Add(HeatEvent{TableName: "recent_table", QueryTime: 3.0, LockTime: 1.0, Timestamp: now.Add(-10 * time.Minute)})

	start := now.Add(-30 * time.Minute)
	end := now.Add(1 * time.Minute)

	top := hm.TopN(start, end, 10)

	for _, s := range top {
		if s.TableName == "old_table" {
			t.Error("old_table should not appear in recent time window")
		}
	}

	found := false
	for _, s := range top {
		if s.TableName == "recent_table" {
			found = true
		}
	}
	if !found {
		t.Error("recent_table should appear in recent time window")
	}
}

func TestHeatMap_HeatScoreFormula(t *testing.T) {
	table := "test_table"
	events := []HeatEvent{
		{TableName: table, QueryTime: 10.0, LockTime: 5.0, Timestamp: time.Now(), SQLType: "UPDATE"},
	}
	hm := NewHeatMap(1*time.Hour, 1000)
	for _, e := range events {
		hm.Add(e)
	}

	now := time.Now().UTC()
	top := hm.TopN(now.Add(-time.Hour), now.Add(time.Hour), 10)
	if len(top) != 1 {
		t.Fatalf("expected 1, got %d", len(top))
	}

	expected := 5.0*2.0 + 10.0 + 1*0.5
	if top[0].HeatScore < expected-0.01 || top[0].HeatScore > expected+0.01 {
		t.Errorf("expected heat score ~%.2f, got %.2f", expected, top[0].HeatScore)
	}
}
