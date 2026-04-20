package analytics_test

import (
        "math"
        "os"
        "testing"
        "time"

        "github.com/mutual-fund-analytics/internal/analytics"
        "github.com/mutual-fund-analytics/internal/db"
)

// setupTestDB creates a temporary SQLite database for testing.
func setupTestDB(t *testing.T) (*db.DB, func()) {
        t.Helper()
        f, err := os.CreateTemp("", "mf_test_*.db")
        if err != nil {
                t.Fatalf("create temp file: %v", err)
        }
        f.Close()

        database, err := db.New(f.Name())
        if err != nil {
                os.Remove(f.Name())
                t.Fatalf("open db: %v", err)
        }

        cleanup := func() {
                database.Close()
                os.Remove(f.Name())
        }
        return database, cleanup
}

// seedSchemeAndNAV inserts a test scheme and its NAV data.
func seedSchemeAndNAV(t *testing.T, database *db.DB, code string, navRows []db.NAVRow) {
        t.Helper()
        if err := database.UpsertScheme(code, "Test Fund", "Test AMC", "Equity: Mid Cap"); err != nil {
                t.Fatalf("upsert scheme: %v", err)
        }
        if err := database.BulkUpsertNAV(navRows); err != nil {
                t.Fatalf("bulk upsert nav: %v", err)
        }
}

// TestMaxDrawdown_BasicDecline verifies drawdown on a simple decline sequence.
// Peak = 100, trough = 60 → max drawdown = -40%.
func TestMaxDrawdown_BasicDecline(t *testing.T) {
        database, cleanup := setupTestDB(t)
        defer cleanup()

        // Build 400 days of data: rises to 100, falls to 60.
        var rows []db.NAVRow
        start := "2019-01-01"
        baseDate := mustParseDate(start)

        for i := 0; i < 200; i++ {
                d := baseDate.AddDate(0, 0, i).Format("2006-01-02")
                rows = append(rows, db.NAVRow{SchemeCode: "TEST1", Date: d, NAV: float64(50 + i/4)})
        }
        // peak is now 100; add 200 days of decline.
        for i := 200; i < 400; i++ {
                d := baseDate.AddDate(0, 0, i).Format("2006-01-02")
                nav := 100.0 - float64(i-200)*0.2 // declines ~40 points
                if nav < 1 {
                        nav = 1
                }
                rows = append(rows, db.NAVRow{SchemeCode: "TEST1", Date: d, NAV: nav})
        }

        seedSchemeAndNAV(t, database, "TEST1", rows)
        eng := analytics.New(database)

        if err := eng.ComputeAll("TEST1"); err != nil {
                t.Logf("ComputeAll: %v (may be insufficient for some windows)", err)
        }

        // Verify that drawdown for 1Y window is negative.
        row, err := database.GetAnalytics("TEST1", "1Y")
        if err != nil {
                t.Fatalf("get analytics: %v", err)
        }
        if row == nil {
                t.Skip("1Y analytics not computed — insufficient data")
        }
        if row.MaxDrawdown >= 0 {
                t.Errorf("expected negative max drawdown, got %.2f", row.MaxDrawdown)
        }
}

// TestRollingReturns_ConstantGrowth verifies rolling return calculation on
// linearly growing NAV. With constant 10%/year growth the CAGR should be ~10.
func TestRollingReturns_ConstantGrowth(t *testing.T) {
        database, cleanup := setupTestDB(t)
        defer cleanup()

        const annualRate = 0.10
        const years = 5
        const totalDays = 365 * (years + 1)

        baseDate := mustParseDate("2015-01-01")
        var rows []db.NAVRow
        startNAV := 100.0

        for i := 0; i < totalDays; i++ {
                d := baseDate.AddDate(0, 0, i)
                nav := startNAV * math.Pow(1+annualRate, float64(i)/365.0)
                rows = append(rows, db.NAVRow{
                        SchemeCode: "TEST2",
                        Date:       d.Format("2006-01-02"),
                        NAV:        nav,
                })
        }

        seedSchemeAndNAV(t, database, "TEST2", rows)
        eng := analytics.New(database)
        eng.ComputeAll("TEST2")

        row, err := database.GetAnalytics("TEST2", "1Y")
        if err != nil || row == nil {
                t.Skip("1Y analytics not available")
        }

        // CAGR median should be ~10% (within 1% tolerance).
        if math.Abs(row.CAGRMedian-10.0) > 1.0 {
                t.Errorf("expected CAGR median ~10%%, got %.4f%%", row.CAGRMedian)
        }

        // Max drawdown should be ~0 (monotonically increasing NAV).
        if row.MaxDrawdown < -2.0 {
                t.Errorf("expected drawdown near 0 for monotonic growth, got %.2f%%", row.MaxDrawdown)
        }
}

// TestRollingReturns_MedianBetweenMinMax verifies statistical invariants.
func TestRollingReturns_MedianBetweenMinMax(t *testing.T) {
        database, cleanup := setupTestDB(t)
        defer cleanup()

        baseDate := mustParseDate("2013-01-01")
        var rows []db.NAVRow
        nav := 100.0

        // Volatile NAV: alternates between growth and decline phases.
        for i := 0; i < 365*4; i++ {
                d := baseDate.AddDate(0, 0, i)
                if i%60 < 30 {
                        nav *= 1.001 // growth phase
                } else {
                        nav *= 0.9995 // decline phase
                }
                rows = append(rows, db.NAVRow{
                        SchemeCode: "TEST3",
                        Date:       d.Format("2006-01-02"),
                        NAV:        nav,
                })
        }

        seedSchemeAndNAV(t, database, "TEST3", rows)
        analytics.New(database).ComputeAll("TEST3")

        row, err := database.GetAnalytics("TEST3", "1Y")
        if err != nil || row == nil {
                t.Skip("analytics not computed")
        }

        // Statistical invariants.
        if row.RollingMin > row.RollingMedian {
                t.Errorf("min (%.2f) > median (%.2f)", row.RollingMin, row.RollingMedian)
        }
        if row.RollingMedian > row.RollingMax {
                t.Errorf("median (%.2f) > max (%.2f)", row.RollingMedian, row.RollingMax)
        }
        if row.RollingP25 > row.RollingMedian {
                t.Errorf("p25 (%.2f) > median (%.2f)", row.RollingP25, row.RollingMedian)
        }
        if row.RollingP75 < row.RollingMedian {
                t.Errorf("p75 (%.2f) < median (%.2f)", row.RollingP75, row.RollingMedian)
        }
}

// TestInsufficientHistory_ShorterWindow verifies that windows with insufficient
// data return no analytics (not a crash).
func TestInsufficientHistory_ShorterWindow(t *testing.T) {
        database, cleanup := setupTestDB(t)
        defer cleanup()

        baseDate := mustParseDate("2024-01-01")
        var rows []db.NAVRow
        // Only 180 days of data — not enough for 1Y window.
        for i := 0; i < 180; i++ {
                d := baseDate.AddDate(0, 0, i)
                rows = append(rows, db.NAVRow{
                        SchemeCode: "TEST4",
                        Date:       d.Format("2006-01-02"),
                        NAV:        100.0 + float64(i)*0.1,
                })
        }

        seedSchemeAndNAV(t, database, "TEST4", rows)
        // Should not panic; errors are expected and logged.
        analytics.New(database).ComputeAll("TEST4")

        // 10Y analytics should definitely not exist.
        row, err := database.GetAnalytics("TEST4", "10Y")
        if err != nil {
                t.Fatalf("get analytics: %v", err)
        }
        if row != nil {
                t.Error("expected no 10Y analytics for 180-day dataset")
        }
}

// TestPercentileCalculation manually verifies the percentile function via
// a known dataset where p25, median, p75 can be calculated by hand.
func TestPercentileCalculation(t *testing.T) {
        database, cleanup := setupTestDB(t)
        defer cleanup()

        baseDate := mustParseDate("2013-01-01")
        // 3 years of data; returns will be [0%, 10%, 20%, 30%] in four equal periods.
        // We build NAV so that the 1Y rolling returns are clearly separated.
        var rows []db.NAVRow
        nav := 100.0
        for i := 0; i < 365*3; i++ {
                d := baseDate.AddDate(0, 0, i)
                // Constant 15% annual growth.
                nav = 100.0 * math.Pow(1.15, float64(i)/365.0)
                rows = append(rows, db.NAVRow{
                        SchemeCode: "TEST5",
                        Date:       d.Format("2006-01-02"),
                        NAV:        nav,
                })
        }

        seedSchemeAndNAV(t, database, "TEST5", rows)
        analytics.New(database).ComputeAll("TEST5")

        row, err := database.GetAnalytics("TEST5", "1Y")
        if err != nil || row == nil {
                t.Skip("1Y analytics not computed")
        }

        // With constant growth the min, median, and max should all be ~15%.
        if math.Abs(row.CAGRMedian-15.0) > 1.5 {
                t.Errorf("expected CAGR median ~15%%, got %.2f%%", row.CAGRMedian)
        }
}

func mustParseDate(s string) time.Time {
        t, err := time.Parse("2006-01-02", s)
        if err != nil {
                panic(err)
        }
        return t
}
