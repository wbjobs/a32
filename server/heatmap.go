package server

import (
	"sort"
	"sync"
	"time"
)

type HeatEvent struct {
	TableName  string
	QueryTime  float64
	LockTime   float64
	Timestamp  time.Time
	Fields     []string
	SQLType    string
}

type TableStats struct {
	TableName       string
	HeatScore       float64
	SlowQueryCount  int64
	TotalQueryTime  float64
	TotalLockTime   float64
	AffectedFields  map[string]int
}

type HeatMap struct {
	mu       sync.RWMutex
	events   []HeatEvent
	window   time.Duration
	maxSize  int
}

func NewHeatMap(window time.Duration, maxSize int) *HeatMap {
	hm := &HeatMap{
		window:  window,
		maxSize: maxSize,
		events:  make([]HeatEvent, 0, maxSize),
	}
	go hm.evictLoop()
	return hm
}

func (hm *HeatMap) Add(event HeatEvent) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.events = append(hm.events, event)
	if len(hm.events) > hm.maxSize {
		hm.events = hm.events[len(hm.events)-hm.maxSize:]
	}
}

func (hm *HeatMap) evictLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		hm.evict()
	}
}

func (hm *HeatMap) evict() {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	cutoff := time.Now().Add(-hm.window)
	i := sort.Search(len(hm.events), func(i int) bool {
		return !hm.events[i].Timestamp.Before(cutoff)
	})
	if i > 0 {
		hm.events = hm.events[i:]
	}
}

func (hm *HeatMap) TopN(start, end time.Time, n int) []TableStats {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	stats := make(map[string]*TableStats)
	totalInWindow := 0

	for _, ev := range hm.events {
		if ev.Timestamp.Before(start) || ev.Timestamp.After(end) {
			continue
		}
		totalInWindow++
		s, ok := stats[ev.TableName]
		if !ok {
			s = &TableStats{
				TableName:      ev.TableName,
				AffectedFields: make(map[string]int),
			}
			stats[ev.TableName] = s
		}
		s.SlowQueryCount++
		s.TotalQueryTime += ev.QueryTime
		s.TotalLockTime += ev.LockTime
		for _, f := range ev.Fields {
			s.AffectedFields[f]++
		}
	}

	for _, s := range stats {
		lockWeight := s.TotalLockTime * 2.0
		timeWeight := s.TotalQueryTime
		countWeight := float64(s.SlowQueryCount) * 0.5
		s.HeatScore = lockWeight + timeWeight + countWeight
	}

	ranked := make([]TableStats, 0, len(stats))
	for _, s := range stats {
		ranked = append(ranked, *s)
	}
	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].HeatScore > ranked[j].HeatScore
	})

	if len(ranked) > n {
		ranked = ranked[:n]
	}

	_ = totalInWindow
	return ranked
}

func (hm *HeatMap) TotalInWindow(start, end time.Time) int {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	count := 0
	for _, ev := range hm.events {
		if !ev.Timestamp.Before(start) && !ev.Timestamp.After(end) {
			count++
		}
	}
	return count
}
