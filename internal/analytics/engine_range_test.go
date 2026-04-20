package analytics_test

import (
	"math"
	"testing"
	"time"

	"github.com/mutual-fund-analytics/internal/analytics"
	"github.com/mutual-fund-analytics/internal/db"
)

// buildDailyNAV creates sequential daily NAV records for a scheme.
// nav(i) = startNAV * (growthRate ^ i/365)  — compound daily growth.
func buildDailyNAV(code string, startDate time.Time, days int, startNAV, annualGrowthRate float64) []db.NAVRow {
	rows := make([]db.NAVRow, days)
	for i := 0; i < days; i++ {
		d := startDate.AddDate(0, 0, i)
		nav := startNAV * math.Pow(1+annualGrowthRate, float64(i)/365.0)
		rows[i] = db.NAVRow{SchemeCode: code, Date: d.Format("2006-01-02"), NAV: nav}
	}
	return rows
}

// TestRangeAnalytics_TotalReturn verifies total return for a simple constant-growth NAV series.
// 3-year range with 10% annual growth → ~33.1% total return.
func TestRangeAnalytics_TotalReturn(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	start := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := buildDailyNAV("RANGE1", start, 4*365, 100.0, 0.10)
	seedSchemeAndNAV(t, database, "RANGE1", rows)

	eng := analytics.New(database)

	// Request 3-year window: 2015-01-01 → 2018-01-01
	from := "2015-01-01"
	to := "2018-01-01"
	result, err := eng.ComputeForDateRange("RANGE1", from, to, "")
	if err != nil {
		t.Fatalf("ComputeForDateRange: %v", err)
	}

	// 10% annual → 3Y total ≈ 33.1%
	expected := (math.Pow(1.10, 3) - 1) * 100
	if math.Abs(result.TotalReturnPct-expected) > 1.0 {
		t.Errorf("TotalReturnPct = %.2f%%, want ~%.2f%%", result.TotalReturnPct, expected)
	}
}

// TestRangeAnalytics_AnnualizedReturn verifies CAGR approximation matches the growth rate.
func TestRangeAnalytics_AnnualizedReturn(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	start := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := buildDailyNAV("RANGE2", start, 5*365, 100.0, 0.15) // 15% annual growth
	seedSchemeAndNAV(t, database, "RANGE2", rows)

	eng := analytics.New(database)

	from := "2015-01-01"
	to := "2020-01-01"
	result, err := eng.ComputeForDateRange("RANGE2", from, to, "")
	if err != nil {
		t.Fatalf("ComputeForDateRange: %v", err)
	}

	// CAGR should be very close to 15%.
	if math.Abs(result.AnnualizedReturnPct-15.0) > 0.5 {
		t.Errorf("AnnualizedReturnPct = %.2f%%, want ~15%%", result.AnnualizedReturnPct)
	}
}

// TestRangeAnalytics_MaxDrawdown verifies the drawdown within a bounded range
// is computed only from the NAV data within that range.
func TestRangeAnalytics_MaxDrawdown(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	// Build a custom NAV: rises 2015→2016, crashes 50% in 2016→2017, recovers 2017→2019.
	start := time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	var rows []db.NAVRow

	// Phase 1: rise from 100 to 200 over 365 days.
	for i := 0; i < 365; i++ {
		d := start.AddDate(0, 0, i)
		nav := 100.0 + float64(i)*100.0/365.0
		rows = append(rows, db.NAVRow{SchemeCode: "RANGE3", Date: d.Format("2006-01-02"), NAV: nav})
	}
	// Phase 2: crash from 200 down to 100 over 365 days.
	for i := 0; i < 365; i++ {
		d := start.AddDate(0, 0, 365+i)
		nav := 200.0 - float64(i)*100.0/365.0
		rows = append(rows, db.NAVRow{SchemeCode: "RANGE3", Date: d.Format("2006-01-02"), NAV: nav})
	}
	// Phase 3: recovery from 100 to 300 over 730 days.
	for i := 0; i < 730; i++ {
		d := start.AddDate(0, 0, 730+i)
		nav := 100.0 + float64(i)*200.0/730.0
		rows = append(rows, db.NAVRow{SchemeCode: "RANGE3", Date: d.Format("2006-01-02"), NAV: nav})
	}

	seedSchemeAndNAV(t, database, "RANGE3", rows)
	eng := analytics.New(database)

	// Query only the crash phase: 2016-01-01 to 2017-01-01.
	// Peak ≈ 200, trough ≈ 100 → max drawdown ≈ -50%.
	result, err := eng.ComputeForDateRange("RANGE3", "2016-01-01", "2017-01-01", "")
	if err != nil {
		t.Fatalf("ComputeForDateRange: %v", err)
	}

	if result.MaxDrawdownPct >= 0 {
		t.Errorf("MaxDrawdownPct should be negative, got %.2f%%", result.MaxDrawdownPct)
	}
	if math.Abs(result.MaxDrawdownPct) < 40 {
		t.Errorf("MaxDrawdownPct = %.2f%%, expected ~-50%%", result.MaxDrawdownPct)
	}
}

// TestRangeAnalytics_InsufficientData verifies error when range has fewer than 2 data points.
func TestRangeAnalytics_InsufficientData(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := buildDailyNAV("RANGE4", start, 10, 100.0, 0.10)
	seedSchemeAndNAV(t, database, "RANGE4", rows)

	eng := analytics.New(database)

	// Request a range with no data in the DB.
	_, err := eng.ComputeForDateRange("RANGE4", "2023-01-01", "2023-12-31", "")
	if err == nil {
		t.Error("expected error for empty range, got nil")
	}
}

// TestRangeAnalytics_NegativeReturn verifies negative total return is computed correctly.
func TestRangeAnalytics_NegativeReturn(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	// NAV falls from 100 to 70 over 180 days.
	start := time.Date(2020, 3, 1, 0, 0, 0, 0, time.UTC)
	var rows []db.NAVRow
	for i := 0; i < 180; i++ {
		d := start.AddDate(0, 0, i)
		nav := 100.0 - float64(i)*30.0/180.0
		rows = append(rows, db.NAVRow{SchemeCode: "RANGE5", Date: d.Format("2006-01-02"), NAV: nav})
	}
	seedSchemeAndNAV(t, database, "RANGE5", rows)

	eng := analytics.New(database)
	result, err := eng.ComputeForDateRange("RANGE5", "2020-03-01", "2020-08-27", "")
	if err != nil {
		t.Fatalf("ComputeForDateRange: %v", err)
	}

	// NAV fell from 100 → 70 → total return ≈ -30%.
	if result.TotalReturnPct >= 0 {
		t.Errorf("expected negative total return, got %.2f%%", result.TotalReturnPct)
	}
	if math.Abs(result.TotalReturnPct+30.0) > 2.0 {
		t.Errorf("TotalReturnPct = %.2f%%, expected ~-30%%", result.TotalReturnPct)
	}
}

// TestRangeAnalytics_RollingWindow verifies rolling sub-window analytics within a range.
func TestRangeAnalytics_RollingWindow(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	// 4 years of 10% annual growth — enough for a 1Y rolling sub-window.
	start := time.Date(2016, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := buildDailyNAV("RANGE6", start, 4*365, 100.0, 0.10)
	seedSchemeAndNAV(t, database, "RANGE6", rows)

	eng := analytics.New(database)
	result, err := eng.ComputeForDateRange("RANGE6", "2016-01-01", "2020-01-01", "1Y")
	if err != nil {
		t.Fatalf("ComputeForDateRange: %v", err)
	}

	if !result.HasRolling {
		t.Fatal("expected HasRolling=true for 4Y range with 1Y sub-window")
	}
	if result.RollingPeriodsAnalyzed < 100 {
		t.Errorf("expected many rolling periods, got %d", result.RollingPeriodsAnalyzed)
	}
	// With consistent 10% growth the median 1Y rolling return should be close to 10%.
	if math.Abs(result.RollingMedian-10.0) > 1.5 {
		t.Errorf("RollingMedian = %.2f%%, expected ~10%%", result.RollingMedian)
	}
}

// TestRangeAnalytics_RollingWindowTooShort verifies no rolling analytics when
// range is shorter than the requested rolling window.
func TestRangeAnalytics_RollingWindowTooShort(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	// Only 6 months of data — too short for a 1Y rolling window.
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := buildDailyNAV("RANGE7", start, 180, 100.0, 0.10)
	seedSchemeAndNAV(t, database, "RANGE7", rows)

	eng := analytics.New(database)
	result, err := eng.ComputeForDateRange("RANGE7", "2020-01-01", "2020-06-28", "1Y")
	if err != nil {
		t.Fatalf("ComputeForDateRange: %v", err)
	}
	if result.HasRolling {
		t.Error("expected HasRolling=false when range is shorter than rolling window")
	}
}

// TestRangeAnalytics_DataAvailabilityFields verifies the ActualFrom/To and NAVDataPoints fields.
func TestRangeAnalytics_DataAvailabilityFields(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	start := time.Date(2019, 6, 1, 0, 0, 0, 0, time.UTC)
	rows := buildDailyNAV("RANGE8", start, 365, 100.0, 0.12)
	seedSchemeAndNAV(t, database, "RANGE8", rows)

	eng := analytics.New(database)
	result, err := eng.ComputeForDateRange("RANGE8", "2019-06-01", "2020-05-31", "")
	if err != nil {
		t.Fatalf("ComputeForDateRange: %v", err)
	}

	if result.NAVDataPoints < 300 {
		t.Errorf("NAVDataPoints = %d, expected ~365", result.NAVDataPoints)
	}
	if result.ActualFrom == "" || result.ActualTo == "" {
		t.Error("ActualFrom or ActualTo is empty")
	}
	if result.CalendarDays <= 0 {
		t.Errorf("CalendarDays = %d, expected positive", result.CalendarDays)
	}
}
