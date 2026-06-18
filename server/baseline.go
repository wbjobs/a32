package server

import (
	"context"
	"math"
	"sync"
	"time"
)

type TableBaseline struct {
	TableName     string
	MeanPerMinute float64
	StdPerMinute  float64
	SampleCount   int
}

type BaselineCalculator struct {
	storage    *Storage
	historyDays int
	mu         sync.RWMutex
	baselines  map[string]*TableBaseline
	stopCh     chan struct{}
}

func NewBaselineCalculator(storage *Storage, historyDays int) *BaselineCalculator {
	return &BaselineCalculator{
		storage:     storage,
		historyDays: historyDays,
		baselines:   make(map[string]*TableBaseline),
		stopCh:      make(chan struct{}),
	}
}

func (bc *BaselineCalculator) Start() {
	bc.refresh()
	go bc.refreshLoop()
}

func (bc *BaselineCalculator) Stop() {
	close(bc.stopCh)
}

func (bc *BaselineCalculator) refreshLoop() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-bc.stopCh:
			return
		case <-ticker.C:
			bc.refresh()
		}
	}
}

func (bc *BaselineCalculator) refresh() {
	since := time.Now().AddDate(0, 0, -bc.historyDays)
	ctx := context.Background()

	allCounts, err := bc.storage.GetAllTableCountsPerMinute(ctx, since)
	if err != nil {
		return
	}

	newBaselines := make(map[string]*TableBaseline)
	for table, counts := range allCounts {
		mean, std, sampleCount := calculateMeanStd(counts)
		newBaselines[table] = &TableBaseline{
			TableName:     table,
			MeanPerMinute: mean,
			StdPerMinute:  std,
			SampleCount:   sampleCount,
		}
	}

	bc.mu.Lock()
	bc.baselines = newBaselines
	bc.mu.Unlock()
}

func calculateMeanStd(counts []PerTableMinuteCount) (mean, std float64, sampleCount int) {
	if len(counts) == 0 {
		return 0, 0, 0
	}

	sum := 0.0
	for _, c := range counts {
		sum += float64(c.Count)
	}
	mean = sum / float64(len(counts))
	sampleCount = len(counts)

	if sampleCount < 2 {
		return mean, 0, sampleCount
	}

	varianceSum := 0.0
	for _, c := range counts {
		diff := float64(c.Count) - mean
		varianceSum += diff * diff
	}
	std = math.Sqrt(varianceSum / float64(sampleCount-1))
	return mean, std, sampleCount
}

func (bc *BaselineCalculator) GetBaseline(tableName string) (*TableBaseline, bool) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	b, ok := bc.baselines[tableName]
	return b, ok
}

type AnomalyDetector struct {
	calculator *BaselineCalculator
	sigmaLevel float64
	mu         sync.RWMutex
	alertsSent map[string]time.Time
	minGap     time.Duration
}

func NewAnomalyDetector(calculator *BaselineCalculator, sigmaLevel float64) *AnomalyDetector {
	return &AnomalyDetector{
		calculator: calculator,
		sigmaLevel: sigmaLevel,
		alertsSent: make(map[string]time.Time),
		minGap:     10 * time.Minute,
	}
}

type AnomalyResult struct {
	TableName     string
	CurrentCount  int
	MeanPerMinute float64
	StdPerMinute  float64
	Threshold     float64
	IsAnomaly     bool
	Severity      string
}

func (ad *AnomalyDetector) Check(tableName string, currentCount int) *AnomalyResult {
	baseline, ok := ad.calculator.GetBaseline(tableName)
	result := &AnomalyResult{
		TableName:    tableName,
		CurrentCount: currentCount,
	}

	if !ok || baseline.SampleCount < 5 {
		result.IsAnomaly = false
		return result
	}

	result.MeanPerMinute = baseline.MeanPerMinute
	result.StdPerMinute = baseline.StdPerMinute
	result.Threshold = baseline.MeanPerMinute + ad.sigmaLevel*baseline.StdPerMinute

	if float64(currentCount) > result.Threshold {
		result.IsAnomaly = true
		if result.StdPerMinute > 0 {
			deviation := (float64(currentCount) - result.MeanPerMinute) / result.StdPerMinute
			if deviation > 5 {
				result.Severity = "critical"
			} else if deviation > 4 {
				result.Severity = "high"
			} else {
				result.Severity = "warning"
			}
		} else {
			result.Severity = "warning"
		}
	}

	return result
}

func (ad *AnomalyDetector) ShouldAlert(tableName string) bool {
	ad.mu.RLock()
	lastAlert, ok := ad.alertsSent[tableName]
	ad.mu.RUnlock()

	if !ok {
		return true
	}
	return time.Since(lastAlert) >= ad.minGap
}

func (ad *AnomalyDetector) MarkAlerted(tableName string) {
	ad.mu.Lock()
	defer ad.mu.Unlock()
	ad.alertsSent[tableName] = time.Now()
}

func (ad *AnomalyDetector) CheckAndMark(tableName string, currentCount int) (*AnomalyResult, bool) {
	result := ad.Check(tableName, currentCount)
	if !result.IsAnomaly {
		return result, false
	}
	if !ad.ShouldAlert(tableName) {
		return result, false
	}
	ad.MarkAlerted(tableName)
	return result, true
}
