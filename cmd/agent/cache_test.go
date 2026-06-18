package main

import (
	"os"
	"path/filepath"
	"testing"

	pb "github.com/trae3/slowquery-lineage/pkg/slowquery"
)

func TestGenerateEntryUUID(t *testing.T) {
	entry1 := &pb.SlowLogEntry{
		Timestamp:    "2026-06-19T10:00:00Z",
		QueryTime:    5.123456,
		LockTime:     0.5,
		Sql:          "SELECT * FROM users WHERE id = 1",
		RowsSent:     1,
		RowsExamined: 100,
	}

	entry2 := &pb.SlowLogEntry{
		Timestamp:    "2026-06-19T10:00:00Z",
		QueryTime:    5.123456,
		LockTime:     0.5,
		Sql:          "SELECT * FROM users WHERE id = 1",
		RowsSent:     1,
		RowsExamined: 100,
	}

	entry3 := &pb.SlowLogEntry{
		Timestamp:    "2026-06-19T10:00:01Z",
		QueryTime:    5.123456,
		LockTime:     0.5,
		Sql:          "SELECT * FROM users WHERE id = 1",
		RowsSent:     1,
		RowsExamined: 100,
	}

	uuid1 := GenerateEntryUUID(entry1)
	uuid2 := GenerateEntryUUID(entry2)
	uuid3 := GenerateEntryUUID(entry3)

	if uuid1 != uuid2 {
		t.Errorf("expected same UUID for identical entries, got %s and %s", uuid1, uuid2)
	}

	if uuid1 == uuid3 {
		t.Errorf("expected different UUID for entries with different timestamps, got same %s", uuid1)
	}

	if uuid1 == "" {
		t.Error("expected non-empty UUID")
	}
}

func TestGenerateEntryUUID_DifferentSQL(t *testing.T) {
	entry1 := &pb.SlowLogEntry{
		Timestamp:    "2026-06-19T10:00:00Z",
		QueryTime:    5.0,
		LockTime:     1.0,
		Sql:          "SELECT * FROM users",
		RowsSent:     10,
		RowsExamined: 100,
	}

	entry2 := &pb.SlowLogEntry{
		Timestamp:    "2026-06-19T10:00:00Z",
		QueryTime:    5.0,
		LockTime:     1.0,
		Sql:          "SELECT * FROM orders",
		RowsSent:     10,
		RowsExamined: 100,
	}

	uuid1 := GenerateEntryUUID(entry1)
	uuid2 := GenerateEntryUUID(entry2)

	if uuid1 == uuid2 {
		t.Error("expected different UUID for different SQL, got same")
	}
}

func TestLocalCache_AddAndRemove(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := NewLocalCache(tmpDir, 10)
	if err != nil {
		t.Fatalf("NewLocalCache error: %v", err)
	}

	entry1 := &pb.SlowLogEntry{Uuid: "uuid-1", Sql: "SELECT 1"}
	entry2 := &pb.SlowLogEntry{Uuid: "uuid-2", Sql: "SELECT 2"}

	if err := cache.Add(entry1); err != nil {
		t.Fatalf("Add error: %v", err)
	}
	if cache.Len() != 1 {
		t.Errorf("expected len 1, got %d", cache.Len())
	}
	if !cache.Has("uuid-1") {
		t.Error("expected uuid-1 to exist")
	}

	if err := cache.Add(entry2); err != nil {
		t.Fatalf("Add error: %v", err)
	}
	if cache.Len() != 2 {
		t.Errorf("expected len 2, got %d", cache.Len())
	}

	if err := cache.Remove("uuid-1"); err != nil {
		t.Fatalf("Remove error: %v", err)
	}
	if cache.Len() != 1 {
		t.Errorf("expected len 1, got %d", cache.Len())
	}
	if cache.Has("uuid-1") {
		t.Error("expected uuid-1 to be removed")
	}
}

func TestLocalCache_DuplicateAdd(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := NewLocalCache(tmpDir, 10)
	if err != nil {
		t.Fatalf("NewLocalCache error: %v", err)
	}

	entry := &pb.SlowLogEntry{Uuid: "uuid-1", Sql: "SELECT 1"}
	if err := cache.Add(entry); err != nil {
		t.Fatalf("Add error: %v", err)
	}
	if err := cache.Add(entry); err != nil {
		t.Fatalf("Add duplicate error: %v", err)
	}
	if cache.Len() != 1 {
		t.Errorf("expected len 1 after duplicate add, got %d", cache.Len())
	}
}

func TestLocalCache_GetAll(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := NewLocalCache(tmpDir, 10)
	if err != nil {
		t.Fatalf("NewLocalCache error: %v", err)
	}

	entries := []*pb.SlowLogEntry{
		{Uuid: "uuid-1", Sql: "SELECT 1"},
		{Uuid: "uuid-2", Sql: "SELECT 2"},
		{Uuid: "uuid-3", Sql: "SELECT 3"},
	}

	for _, e := range entries {
		cache.Add(e)
	}

	all := cache.GetAll()
	if len(all) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(all))
	}

	order := []string{all[0].Uuid, all[1].Uuid, all[2].Uuid}
	expected := []string{"uuid-1", "uuid-2", "uuid-3"}
	for i := range order {
		if order[i] != expected[i] {
			t.Errorf("expected order %v, got %v", expected, order)
			break
		}
	}
}

func TestLocalCache_Eviction(t *testing.T) {
	tmpDir := t.TempDir()
	maxSize := 3

	cache, err := NewLocalCache(tmpDir, maxSize)
	if err != nil {
		t.Fatalf("NewLocalCache error: %v", err)
	}

	for i := 0; i < 5; i++ {
		entry := &pb.SlowLogEntry{Uuid: "uuid-" + string(rune('0'+i)), Sql: "SELECT 1"}
		cache.Add(entry)
	}

	if cache.Len() != maxSize {
		t.Errorf("expected len %d, got %d", maxSize, cache.Len())
	}

	if cache.Has("uuid-0") {
		t.Error("expected oldest entry uuid-0 to be evicted")
	}
	if cache.Has("uuid-1") {
		t.Error("expected oldest entry uuid-1 to be evicted")
	}
	if !cache.Has("uuid-2") {
		t.Error("expected uuid-2 to exist")
	}
	if !cache.Has("uuid-3") {
		t.Error("expected uuid-3 to exist")
	}
	if !cache.Has("uuid-4") {
		t.Error("expected uuid-4 to exist")
	}
}

func TestLocalCache_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	cache1, err := NewLocalCache(tmpDir, 10)
	if err != nil {
		t.Fatalf("NewLocalCache error: %v", err)
	}

	cache1.Add(&pb.SlowLogEntry{Uuid: "uuid-1", Sql: "SELECT 1"})
	cache1.Add(&pb.SlowLogEntry{Uuid: "uuid-2", Sql: "SELECT 2"})

	if err := cache1.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	cache2, err := NewLocalCache(tmpDir, 10)
	if err != nil {
		t.Fatalf("NewLocalCache error: %v", err)
	}

	if cache2.Len() != 2 {
		t.Errorf("expected len 2 after reload, got %d", cache2.Len())
	}
	if !cache2.Has("uuid-1") || !cache2.Has("uuid-2") {
		t.Error("expected both uuids to persist")
	}

	all := cache2.GetAll()
	if len(all) != 2 || all[0].Uuid != "uuid-1" || all[1].Uuid != "uuid-2" {
		t.Errorf("expected order to persist, got %v", all)
	}
}

func TestLocalCache_AddWithoutUUID(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := NewLocalCache(tmpDir, 10)
	if err != nil {
		t.Fatalf("NewLocalCache error: %v", err)
	}

	entry := &pb.SlowLogEntry{Sql: "SELECT 1"}
	err = cache.Add(entry)
	if err == nil {
		t.Error("expected error for entry without uuid")
	}
}

func TestLocalCache_ConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := NewLocalCache(tmpDir, 100)
	if err != nil {
		t.Fatalf("NewLocalCache error: %v", err)
	}

	done := make(chan bool, 2)

	go func() {
		for i := 0; i < 50; i++ {
			entry := &pb.SlowLogEntry{Uuid: "g1-uuid-" + string(rune('0'+i)), Sql: "SELECT 1"}
			cache.Add(entry)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 50; i++ {
			entry := &pb.SlowLogEntry{Uuid: "g2-uuid-" + string(rune('0'+i)), Sql: "SELECT 1"}
			cache.Add(entry)
		}
		done <- true
	}()

	<-done
	<-done

	if cache.Len() != 100 {
		t.Errorf("expected 100 entries, got %d", cache.Len())
	}
}

func TestFnv1a64(t *testing.T) {
	hash1 := fnv1a64("test string 1")
	hash2 := fnv1a64("test string 1")
	hash3 := fnv1a64("test string 2")

	if hash1 != hash2 {
		t.Errorf("expected same hash for same input, got %d and %d", hash1, hash2)
	}
	if hash1 == hash3 {
		t.Error("expected different hash for different input, got same")
	}
	if hash1 == 0 {
		t.Error("expected non-zero hash")
	}
}

func TestLocalCache_CreateDir(t *testing.T) {
	baseDir := t.TempDir()
	nestedDir := filepath.Join(baseDir, "a", "b", "c")

	cache, err := NewLocalCache(nestedDir, 10)
	if err != nil {
		t.Fatalf("NewLocalCache error: %v", err)
	}

	if _, err := os.Stat(nestedDir); os.IsNotExist(err) {
		t.Error("expected cache directory to be created")
	}

	if cache != nil {
		cache.Close()
	}
}
