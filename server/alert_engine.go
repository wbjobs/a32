package server

import (
	"context"
	"log"
	"sync"
	"time"
)

type PerMinuteCounter struct {
	mu            sync.RWMutex
	currentMinute string
	counts        map[string]int
	prevMinute    string
	prevCounts    map[string]int
}

func NewPerMinuteCounter() *PerMinuteCounter {
	now := time.Now().UTC()
	return &PerMinuteCounter{
		currentMinute: minuteBucket(now),
		counts:        make(map[string]int),
		prevMinute:    minuteBucket(now.Add(-1 * time.Minute)),
		prevCounts:    make(map[string]int),
	}
}

func (c *PerMinuteCounter) Record(tableName string) {
	now := time.Now().UTC()
	bucket := minuteBucket(now)

	c.mu.Lock()
	defer c.mu.Unlock()

	if bucket != c.currentMinute {
		c.prevMinute = c.currentMinute
		c.prevCounts = c.counts
		c.currentMinute = bucket
		c.counts = make(map[string]int)
	}
	c.counts[tableName]++
}

func (c *PerMinuteCounter) GetCurrentMinute() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentMinute
}

func (c *PerMinuteCounter) GetCurrentCount(tableName string) (int, string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.counts[tableName], c.currentMinute
}

func (c *PerMinuteCounter) GetAllCurrentCounts() map[string]int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]int, len(c.counts))
	for k, v := range c.counts {
		result[k] = v
	}
	return result
}

type AlertEngine struct {
	counter    *PerMinuteCounter
	detector   *AnomalyDetector
	notifier   *DingTalkNotifier
	stopCh     chan struct{}
}

func NewAlertEngine(counter *PerMinuteCounter, detector *AnomalyDetector, notifier *DingTalkNotifier) *AlertEngine {
	return &AlertEngine{
		counter:  counter,
		detector: detector,
		notifier: notifier,
		stopCh:   make(chan struct{}),
	}
}

func (ae *AlertEngine) Start() {
	go ae.run()
}

func (ae *AlertEngine) Stop() {
	close(ae.stopCh)
}

func (ae *AlertEngine) run() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ae.stopCh:
			return
		case <-ticker.C:
			ae.checkAndAlert()
		}
	}
}

func (ae *AlertEngine) checkAndAlert() {
	ctx := context.Background()
	counts := ae.counter.GetAllCurrentCounts()

	for table, count := range counts {
		result, shouldAlert := ae.detector.CheckAndMark(table, count)
		if shouldAlert {
			log.Printf("[alert] anomaly detected: table=%s count=%d mean=%.2f std=%.2f threshold=%.2f severity=%s",
				table, count, result.MeanPerMinute, result.StdPerMinute, result.Threshold, result.Severity)

			if err := ae.notifier.SendAnomalyAlert(ctx, result); err != nil {
				log.Printf("[alert] failed to send dingtalk alert for %s: %v", table, err)
			}
		}
	}
}
