package server

import (
	"context"
	"testing"
	"time"

	pb "github.com/trae3/slowquery-lineage/pkg/slowquery"
	"github.com/redis/go-redis/v9"
)

type mockRedisClient struct {
	data map[string]interface{}
	ttl  map[string]time.Duration
}

func newMockRedisClient() *mockRedisClient {
	return &mockRedisClient{
		data: make(map[string]interface{}),
		ttl:  make(map[string]time.Duration),
	}
}

func (m *mockRedisClient) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolResult(false, nil)
	if _, exists := m.data[key]; !exists {
		m.data[key] = value
		m.ttl[key] = expiration
		cmd = redis.NewBoolResult(true, nil)
	}
	return cmd
}

func (m *mockRedisClient) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	count := int64(0)
	for _, k := range keys {
		if _, exists := m.data[k]; exists {
			count++
		}
	}
	return redis.NewIntResult(count, nil)
}

func (m *mockRedisClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	count := int64(0)
	for _, k := range keys {
		if _, exists := m.data[k]; exists {
			delete(m.data, k)
			delete(m.ttl, k)
			count++
		}
	}
	return redis.NewIntResult(count, nil)
}

func (m *mockRedisClient) Ping(ctx context.Context) *redis.StatusCmd {
	return redis.NewStatusResult("PONG", nil)
}

func (m *mockRedisClient) Close() error {
	return nil
}

func TestIdempotencyCheckAndSet(t *testing.T) {
	mock := newMockRedisClient()
	store := NewIdempotencyStoreWithClient(mock, 24*time.Hour)

	ctx := context.Background()

	ok, err := store.CheckAndSet(ctx, "uuid-123")
	if err != nil {
		t.Fatalf("CheckAndSet error: %v", err)
	}
	if !ok {
		t.Error("expected first CheckAndSet to return true")
	}

	ok, err = store.CheckAndSet(ctx, "uuid-123")
	if err != nil {
		t.Fatalf("CheckAndSet error: %v", err)
	}
	if ok {
		t.Error("expected second CheckAndSet to return false (duplicate)")
	}

	ok, err = store.CheckAndSet(ctx, "uuid-456")
	if err != nil {
		t.Fatalf("CheckAndSet error: %v", err)
	}
	if !ok {
		t.Error("expected new uuid to return true")
	}

	ok, err = store.Exists(ctx, "uuid-123")
	if err != nil {
		t.Fatalf("Exists error: %v", err)
	}
	if !ok {
		t.Error("expected uuid-123 to exist")
	}

	if err := store.Clear(ctx, "uuid-123"); err != nil {
		t.Fatalf("Clear error: %v", err)
	}

	ok, err = store.Exists(ctx, "uuid-123")
	if err != nil {
		t.Fatalf("Exists error: %v", err)
	}
	if ok {
		t.Error("expected uuid-123 to be cleared")
	}
}

func TestIdempotencyEmptyUUID(t *testing.T) {
	mock := newMockRedisClient()
	store := NewIdempotencyStoreWithClient(mock, 24*time.Hour)

	ctx := context.Background()

	ok, err := store.CheckAndSet(ctx, "")
	if err != nil {
		t.Fatalf("CheckAndSet error: %v", err)
	}
	if !ok {
		t.Error("expected empty uuid to return true (pass through)")
	}

	ok, err = store.Exists(ctx, "")
	if err != nil {
		t.Fatalf("Exists error: %v", err)
	}
	if ok {
		t.Error("expected empty uuid to not exist in store")
	}
}

func TestIdempotencyGRPCIntegration(t *testing.T) {
	hm := NewHeatMap(1*time.Hour, 10000)
	svc := NewGRPCService(hm, nil)

	entry := &pb.SlowLogEntry{
		AgentId:    "agent-1",
		Timestamp:  "2026-06-19T10:00:01Z",
		QueryTime:  5.23,
		LockTime:   3.10,
		Sql:        "SELECT id FROM users WHERE status = 'active'",
		Database:   "myapp",
		Uuid:       "test-uuid-001",
	}

	events := svc.processEntry(entry)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	events2 := svc.processEntry(entry)
	if len(events2) != 1 {
		t.Fatalf("expected 1 event for duplicate (no server-side check yet), got %d", len(events2))
	}
}
