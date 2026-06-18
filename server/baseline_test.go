package server

import (
	"math"
	"testing"
)

func TestCalculateMeanStd(t *testing.T) {
	counts := []PerTableMinuteCount{
		{TableName: "users", MinuteBucket: "2026-06-19 10:00", Count: 2},
		{TableName: "users", MinuteBucket: "2026-06-19 10:01", Count: 4},
		{TableName: "users", MinuteBucket: "2026-06-19 10:02", Count: 6},
		{TableName: "users", MinuteBucket: "2026-06-19 10:03", Count: 4},
		{TableName: "users", MinuteBucket: "2026-06-19 10:04", Count: 2},
	}

	mean, std, n := calculateMeanStd(counts)

	if n != 5 {
		t.Errorf("expected sample count 5, got %d", n)
	}

	expectedMean := 3.6
	if math.Abs(mean-expectedMean) > 0.001 {
		t.Errorf("expected mean %.2f, got %.2f", expectedMean, mean)
	}

	expectedStd := 1.6733
	if math.Abs(std-expectedStd) > 0.01 {
		t.Errorf("expected std %.2f, got %.2f", expectedStd, std)
	}
}

func TestCalculateMeanStd_SingleSample(t *testing.T) {
	counts := []PerTableMinuteCount{
		{TableName: "users", MinuteBucket: "2026-06-19 10:00", Count: 5},
	}

	mean, std, n := calculateMeanStd(counts)

	if n != 1 {
		t.Errorf("expected sample count 1, got %d", n)
	}
	if mean != 5.0 {
		t.Errorf("expected mean 5.0, got %.2f", mean)
	}
	if std != 0.0 {
		t.Errorf("expected std 0.0 for single sample, got %.2f", std)
	}
}

func TestCalculateMeanStd_Empty(t *testing.T) {
	counts := []PerTableMinuteCount{}
	mean, std, n := calculateMeanStd(counts)

	if n != 0 {
		t.Errorf("expected sample count 0, got %d", n)
	}
	if mean != 0.0 {
		t.Errorf("expected mean 0.0, got %.2f", mean)
	}
	if std != 0.0 {
		t.Errorf("expected std 0.0, got %.2f", std)
	}
}

func TestAnomalyDetector(t *testing.T) {
	counts := []PerTableMinuteCount{
		{TableName: "users", MinuteBucket: "2026-06-19 10:00", Count: 2},
		{TableName: "users", MinuteBucket: "2026-06-19 10:01", Count: 4},
		{TableName: "users", MinuteBucket: "2026-06-19 10:02", Count: 6},
		{TableName: "users", MinuteBucket: "2026-06-19 10:03", Count: 4},
		{TableName: "users", MinuteBucket: "2026-06-19 10:04", Count: 2},
		{TableName: "users", MinuteBucket: "2026-06-19 10:05", Count: 5},
		{TableName: "users", MinuteBucket: "2026-06-19 10:06", Count: 3},
	}

	baselineMap := make(map[string]*TableBaseline)
	mean, std, _ := calculateMeanStd(counts)
	baselineMap["users"] = &TableBaseline{
		TableName:     "users",
		MeanPerMinute: mean,
		StdPerMinute:  std,
		SampleCount:   len(counts),
	}

	calc := &BaselineCalculator{
		baselines: baselineMap,
	}

	detector := NewAnomalyDetector(calc, 3.0)

	normalResult := detector.Check("users", 3)
	if normalResult.IsAnomaly {
		t.Error("expected count=3 to be normal, but got anomaly")
	}

	anomalyResult := detector.Check("users", 50)
	if !anomalyResult.IsAnomaly {
		t.Error("expected count=50 to be anomaly, but got normal")
	}
	if anomalyResult.Severity != "critical" {
		t.Errorf("expected critical severity, got %s", anomalyResult.Severity)
	}
}

func TestAnomalyDetector_NoBaseline(t *testing.T) {
	calc := &BaselineCalculator{
		baselines: make(map[string]*TableBaseline),
	}
	detector := NewAnomalyDetector(calc, 3.0)

	result := detector.Check("unknown_table", 100)
	if result.IsAnomaly {
		t.Error("expected no anomaly for table without baseline")
	}
}

func TestAnomalyDetector_ShouldAlertRateLimit(t *testing.T) {
	counts := []PerTableMinuteCount{
		{TableName: "users", MinuteBucket: "2026-06-19 10:00", Count: 2},
		{TableName: "users", MinuteBucket: "2026-06-19 10:01", Count: 3},
		{TableName: "users", MinuteBucket: "2026-06-19 10:02", Count: 2},
		{TableName: "users", MinuteBucket: "2026-06-19 10:03", Count: 3},
		{TableName: "users", MinuteBucket: "2026-06-19 10:04", Count: 2},
	}

	baselineMap := make(map[string]*TableBaseline)
	mean, std, _ := calculateMeanStd(counts)
	baselineMap["users"] = &TableBaseline{
		TableName:     "users",
		MeanPerMinute: mean,
		StdPerMinute:  std,
		SampleCount:   len(counts),
	}

	calc := &BaselineCalculator{
		baselines: baselineMap,
	}

	detector := NewAnomalyDetector(calc, 3.0)

	if !detector.ShouldAlert("users") {
		t.Error("expected first alert to be allowed")
	}

	detector.MarkAlerted("users")

	if detector.ShouldAlert("users") {
		t.Error("expected second alert to be rate-limited")
	}
}

func TestPerMinuteCounter(t *testing.T) {
	counter := NewPerMinuteCounter()

	counter.Record("users")
	counter.Record("users")
	counter.Record("orders")

	count, _ := counter.GetCurrentCount("users")
	if count != 2 {
		t.Errorf("expected users count 2, got %d", count)
	}

	count, _ = counter.GetCurrentCount("orders")
	if count != 1 {
		t.Errorf("expected orders count 1, got %d", count)
	}

	count, _ = counter.GetCurrentCount("unknown")
	if count != 0 {
		t.Errorf("expected unknown count 0, got %d", count)
	}

	allCounts := counter.GetAllCurrentCounts()
	if len(allCounts) != 2 {
		t.Errorf("expected 2 tables in counts, got %d", len(allCounts))
	}
}
