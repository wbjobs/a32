package main

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/trae3/slowquery-lineage/pkg/slowquery"
)

type PendingEntry struct {
	Entry     *pb.SlowLogEntry
	Timestamp time.Time
}

type LocalCache struct {
	mu       sync.RWMutex
	dataDir  string
	maxSize  int
	pending  map[string]*PendingEntry
	order    []string
	encoder  *gob.Encoder
	decoder  *gob.Decoder
}

func NewLocalCache(dataDir string, maxSize int) (*LocalCache, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	cache := &LocalCache{
		dataDir: dataDir,
		maxSize: maxSize,
		pending: make(map[string]*PendingEntry),
		order:   make([]string, 0, maxSize),
	}

	if err := cache.load(); err != nil {
		return nil, err
	}

	return cache, nil
}

func (c *LocalCache) snapshotFile() string {
	return filepath.Join(c.dataDir, "pending.gob")
}

func (c *LocalCache) Add(entry *pb.SlowLogEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	uuid := entry.GetUuid()
	if uuid == "" {
		return fmt.Errorf("entry has no uuid")
	}

	if _, exists := c.pending[uuid]; exists {
		return nil
	}

	for len(c.pending) >= c.maxSize {
		oldest := c.order[0]
		delete(c.pending, oldest)
		c.order = c.order[1:]
	}

	c.pending[uuid] = &PendingEntry{
		Entry:     entry,
		Timestamp: time.Now(),
	}
	c.order = append(c.order, uuid)

	return c.persist()
}

func (c *LocalCache) Remove(uuid string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.pending[uuid]; !exists {
		return nil
	}

	delete(c.pending, uuid)
	for i, o := range c.order {
		if o == uuid {
			c.order = append(c.order[:i], c.order[i+1:]...)
			break
		}
	}

	return c.persist()
}

func (c *LocalCache) GetAll() []*pb.SlowLogEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*pb.SlowLogEntry, 0, len(c.order))
	for _, uuid := range c.order {
		if pe, ok := c.pending[uuid]; ok {
			result = append(result, pe.Entry)
		}
	}
	return result
}

func (c *LocalCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.pending)
}

func (c *LocalCache) Has(uuid string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.pending[uuid]
	return ok
}

type cacheSnapshot struct {
	Pending map[string]*PendingEntry
	Order   []string
}

func (c *LocalCache) persist() error {
	f, err := os.Create(c.snapshotFile())
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	snap := cacheSnapshot{
		Pending: c.pending,
		Order:   c.order,
	}
	return enc.Encode(snap)
}

func (c *LocalCache) load() error {
	f, err := os.Open(c.snapshotFile())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open snapshot: %w", err)
	}
	defer f.Close()

	dec := gob.NewDecoder(f)
	var snap cacheSnapshot
	if err := dec.Decode(&snap); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	c.pending = snap.Pending
	if c.pending == nil {
		c.pending = make(map[string]*PendingEntry)
	}
	c.order = snap.Order
	if c.order == nil {
		c.order = make([]string, 0, c.maxSize)
	}

	return nil
}

func (c *LocalCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.persist()
}

func GenerateEntryUUID(entry *pb.SlowLogEntry) string {
	sampleRate := 1.0
	if entry.QueryTime >= 10.0 {
		sampleRate = 1.0
	} else if entry.QueryTime >= 1.0 {
		sampleRate = 1.0
	} else {
		sampleRate = 1.0
	}

	content := fmt.Sprintf("%s|%.6f|%.6f|%s|%d|%d|%.3f",
		entry.GetTimestamp(),
		entry.GetQueryTime(),
		entry.GetLockTime(),
		entry.GetSql(),
		entry.GetRowsSent(),
		entry.GetRowsExamined(),
		sampleRate,
	)
	return fmt.Sprintf("%x", fnv1a64(content))
}

func fnv1a64(s string) uint64 {
	const (
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)
	hash := offset64
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime64
	}
	return hash
}
