// Package analytics pre-computes rolling returns, drawdown, and CAGR
// distributions for each tracked mutual fund over 1Y/3Y/5Y/10Y windows.
package analytics

import (
        "fmt"
        "log"
        "math"
        "sort"
        "time"

        "github.com/mutual-fund-analytics/internal/db"
        "github.com/mutual-fund-analytics/internal/models"
)

// Engine computes and persists analytics.
type Engine struct {
        db *db.DB
}

// New returns an Engine.
func New(database *db.DB) *Engine {
        return &Engine{db: database}
}

// ComputeAll computes analytics for all windows for a given scheme.
func (e *Engine) ComputeAll(schemeCode string) error {
        nav, err := e.db.GetNAVHistory(schemeCode)
        if err != nil {
                return fmt.Errorf("get NAV history for %s: %w", schemeCode, err)
        }
        if len(nav) == 0 {
                return fmt.Errorf("no NAV data for scheme %s", schemeCode)
        }

        windows := []models.AnalyticsWindow{
                models.Window1Y,
                models.Window3Y,
                models.Window5Y,
                models.Window10Y,
        }

        for _, w := range windows {
                if err := e.computeWindow(schemeCode, nav, w); err != nil {
                        // Log and continue — insufficient history is expected for some windows.
                        log.Printf("[analytics] %s/%s: %v", schemeCode, w, err)
                }
        }
        return nil
}

// computeWindow computes and stores analytics for one scheme+window pair.
func (e *Engine) computeWindow(schemeCode string, nav []db.NAVRow, window models.AnalyticsWindow) error {
        years := models.WindowYears[window]
        if years == 0 {
                return fmt.Errorf("unknown window %s", window)
        }

        // Parse dates and NAVs into aligned slices.
        dates, navValues, err := parseNAV(nav)
        if err != nil {
                return err
        }

        n := len(dates)
        if n < 2 {
                return fmt.Errorf("insufficient data points (%d) for window %s", n, window)
        }

        windowDays := int(years * 365)

        // Require at least windowDays worth of data.
        totalCalendarDays := int(dates[n-1].Sub(dates[0]).Hours() / 24)
        if totalCalendarDays < windowDays {
                return fmt.Errorf("only %d calendar days available, need %d for %s window",
                        totalCalendarDays, windowDays, window)
        }

        // ---- Rolling returns and CAGR ----------------------------------------
        // For each trading day i, look back ~windowDays calendar days to find
        // the closest available NAV date. Compute:
        //   total return (%) = (nav[i] / nav[j] - 1) * 100
        //   CAGR (%)         = ((nav[i]/nav[j])^(1/years) - 1) * 100
        var rollingReturns []float64
        var cagrs []float64

        // Build a map date→navValue for O(1) lookup.
        dateIdx := make(map[string]int, n)
        for i, d := range dates {
                dateIdx[d.Format("2006-01-02")] = i
        }

        for i := 0; i < n; i++ {
                target := dates[i].AddDate(0, 0, -windowDays)
                // Find the closest trading day on or after target.
                j := findNearestDate(dates, target)
                if j < 0 || j >= i {
                        continue
                }

                // Verify date gap is close enough (within 10 trading-day tolerance).
                gap := dates[i].Sub(dates[j])
                minGap := time.Duration(float64(windowDays-10)*24) * time.Hour
                maxGap := time.Duration(float64(windowDays+30)*24) * time.Hour
                if gap < minGap || gap > maxGap {
                        continue
                }

                startNAV := navValues[j]
                endNAV := navValues[i]
                if startNAV <= 0 {
                        continue
                }

                ratio := endNAV / startNAV
                totalReturn := (ratio - 1) * 100
                cagr := (math.Pow(ratio, 1.0/years) - 1) * 100

                rollingReturns = append(rollingReturns, totalReturn)
                cagrs = append(cagrs, cagr)
        }

        minPeriods := 12 // at least 12 rolling periods required
        if len(rollingReturns) < minPeriods {
                return fmt.Errorf("only %d rolling periods found (need %d) for %s window",
                        len(rollingReturns), minPeriods, window)
        }

        // ---- Max drawdown ------------------------------------------------------
        maxDrawdown := computeMaxDrawdown(navValues)

        // ---- Compute stats -----------------------------------------------------
        rStats := computeStats(rollingReturns)
        cStats := computeCAGRStats(cagrs)

        // ---- Persist -----------------------------------------------------------
        startDate := dates[0].Format("2006-01-02")
        endDate := dates[n-1].Format("2006-01-02")

        row := db.AnalyticsRow{
                SchemeCode:             schemeCode,
                Window:                 string(window),
                RollingMin:             rStats.Min,
                RollingMax:             rStats.Max,
                RollingMedian:          rStats.Median,
                RollingP25:             rStats.P25,
                RollingP75:             rStats.P75,
                MaxDrawdown:            maxDrawdown,
                CAGRMin:                cStats.Min,
                CAGRMax:                cStats.Max,
                CAGRMedian:             cStats.Median,
                RollingPeriodsAnalyzed: len(rollingReturns),
                StartDate:              startDate,
                EndDate:                endDate,
                TotalDays:              totalCalendarDays,
                NAVDataPoints:          n,
                ComputedAt:             time.Now().UTC().Format(time.RFC3339),
        }

        if err := e.db.UpsertAnalytics(row); err != nil {
                return fmt.Errorf("persist analytics: %w", err)
        }

        log.Printf("[analytics] %s/%s: %d rolling periods, max_drawdown=%.2f%%, median_return=%.2f%%",
                schemeCode, window, len(rollingReturns), maxDrawdown, rStats.Median)
        return nil
}

// computeMaxDrawdown returns the worst peak-to-trough decline as a percentage.
// The result is negative (e.g., -32.1 means a 32.1% drawdown).
func computeMaxDrawdown(navs []float64) float64 {
        if len(navs) == 0 {
                return 0
        }
        peak := navs[0]
        maxDD := 0.0

        for _, nav := range navs {
                if nav > peak {
                        peak = nav
                }
                dd := (nav - peak) / peak * 100
                if dd < maxDD {
                        maxDD = dd
                }
        }
        return maxDD
}

// stats holds distributional metrics for a series of returns.
type stats struct {
        Min    float64
        Max    float64
        Median float64
        P25    float64
        P75    float64
}

type cagrStats struct {
        Min    float64
        Max    float64
        Median float64
}

func computeStats(values []float64) stats {
        if len(values) == 0 {
                return stats{}
        }
        sorted := make([]float64, len(values))
        copy(sorted, values)
        sort.Float64s(sorted)

        return stats{
                Min:    sorted[0],
                Max:    sorted[len(sorted)-1],
                Median: percentile(sorted, 50),
                P25:    percentile(sorted, 25),
                P75:    percentile(sorted, 75),
        }
}

func computeCAGRStats(values []float64) cagrStats {
        if len(values) == 0 {
                return cagrStats{}
        }
        sorted := make([]float64, len(values))
        copy(sorted, values)
        sort.Float64s(sorted)

        return cagrStats{
                Min:    sorted[0],
                Max:    sorted[len(sorted)-1],
                Median: percentile(sorted, 50),
        }
}

// percentile returns the p-th percentile (0-100) of an already-sorted slice
// using linear interpolation.
func percentile(sorted []float64, p float64) float64 {
        n := len(sorted)
        if n == 0 {
                return 0
        }
        if n == 1 {
                return sorted[0]
        }

        idx := (p / 100) * float64(n-1)
        lo := int(math.Floor(idx))
        hi := int(math.Ceil(idx))
        if lo == hi {
                return sorted[lo]
        }
        frac := idx - float64(lo)
        return sorted[lo]*(1-frac) + sorted[hi]*frac
}

// findNearestDate returns the index in dates of the closest date to target
// that is >= target. Returns -1 if none found.
func findNearestDate(dates []time.Time, target time.Time) int {
        lo, hi := 0, len(dates)-1
        for lo <= hi {
                mid := (lo + hi) / 2
                if dates[mid].Equal(target) {
                        return mid
                } else if dates[mid].Before(target) {
                        lo = mid + 1
                } else {
                        hi = mid - 1
                }
        }
        // lo is now the first index with dates[lo] >= target
        if lo >= len(dates) {
                return -1
        }
        return lo
}

// parseNAV converts NAVRow slice into parallel date/nav slices.
func parseNAV(rows []db.NAVRow) ([]time.Time, []float64, error) {
        dates := make([]time.Time, 0, len(rows))
        navs := make([]float64, 0, len(rows))
        for _, r := range rows {
                t, err := time.Parse("2006-01-02", r.Date)
                if err != nil {
                        continue // skip malformed dates
                }
                if r.NAV <= 0 {
                        continue // skip zero/negative NAVs
                }
                dates = append(dates, t)
                navs = append(navs, r.NAV)
        }
        if len(dates) == 0 {
                return nil, nil, fmt.Errorf("no valid NAV entries")
        }
        return dates, navs, nil
}

// Round rounds a float64 to 2 decimal places.
func Round2(v float64) float64 {
        return math.Round(v*100) / 100
}

// RangeResult is the on-the-fly analytics result for an arbitrary date range.
// It is never persisted — computed fresh on each request.
type RangeResult struct {
        ActualFrom          string
        ActualTo            string
        CalendarDays        int
        NAVDataPoints       int
        StartNAV            float64
        EndNAV              float64
        TotalReturnPct      float64
        AnnualizedReturnPct float64
        MaxDrawdownPct      float64

        // Rolling sub-window results — nil if no rolling_window was requested
        // or if the range is too short for the requested window.
        RollingWindow           string
        RollingPeriodsAnalyzed  int
        RollingMin              float64
        RollingMax              float64
        RollingMedian           float64
        RollingP25              float64
        RollingP75              float64
        CAGRMin                 float64
        CAGRMax                 float64
        CAGRMedian              float64
        HasRolling              bool
}

// ComputeForDateRange computes analytics for schemeCode over [fromDate, toDate].
// fromDate and toDate must be "YYYY-MM-DD". rollingWindow may be "" (no rolling).
// Returns an error if there are fewer than 2 NAV data points in the range.
func (e *Engine) ComputeForDateRange(schemeCode, fromDate, toDate, rollingWindow string) (*RangeResult, error) {
        nav, err := e.db.GetNAVHistoryForRange(schemeCode, fromDate, toDate)
        if err != nil {
                return nil, fmt.Errorf("get NAV range for %s: %w", schemeCode, err)
        }
        if len(nav) < 2 {
                return nil, fmt.Errorf("only %d NAV data points in range %s–%s (need at least 2)", len(nav), fromDate, toDate)
        }

        dates, navValues, err := parseNAV(nav)
        if err != nil {
                return nil, err
        }
        n := len(dates)

        startNAV := navValues[0]
        endNAV := navValues[n-1]

        calendarDays := int(dates[n-1].Sub(dates[0]).Hours() / 24)

        // Total return.
        ratio := endNAV / startNAV
        totalReturn := (ratio - 1) * 100

        // Annualized return (CAGR) using actual calendar days.
        years := float64(calendarDays) / 365.0
        var annualized float64
        if years > 0 {
                annualized = (math.Pow(ratio, 1.0/years) - 1) * 100
        }

        result := &RangeResult{
                ActualFrom:          dates[0].Format("2006-01-02"),
                ActualTo:            dates[n-1].Format("2006-01-02"),
                CalendarDays:        calendarDays,
                NAVDataPoints:       n,
                StartNAV:            startNAV,
                EndNAV:              endNAV,
                TotalReturnPct:      totalReturn,
                AnnualizedReturnPct: annualized,
                MaxDrawdownPct:      computeMaxDrawdown(navValues),
        }

        // Optional rolling sub-window.
        if rollingWindow != "" {
                win, ok := models.ValidWindows[rollingWindow]
                if !ok {
                        return nil, fmt.Errorf("invalid rolling_window %q; must be one of 1Y, 3Y, 5Y, 10Y", rollingWindow)
                }
                rollingYears := models.WindowYears[win]
                windowDays := int(rollingYears * 365)

                if calendarDays >= windowDays {
                        var rollingReturns, cagrs []float64
                        for i := 0; i < n; i++ {
                                target := dates[i].AddDate(0, 0, -windowDays)
                                j := findNearestDate(dates, target)
                                if j < 0 || j >= i {
                                        continue
                                }
                                gap := dates[i].Sub(dates[j])
                                minGap := time.Duration(float64(windowDays-10)*24) * time.Hour
                                maxGap := time.Duration(float64(windowDays+30)*24) * time.Hour
                                if gap < minGap || gap > maxGap {
                                        continue
                                }
                                if navValues[j] <= 0 {
                                        continue
                                }
                                r := navValues[i] / navValues[j]
                                rollingReturns = append(rollingReturns, (r-1)*100)
                                cagrs = append(cagrs, (math.Pow(r, 1.0/rollingYears)-1)*100)
                        }

                        if len(rollingReturns) >= 1 {
                                rStats := computeStats(rollingReturns)
                                cStats := computeCAGRStats(cagrs)
                                result.HasRolling = true
                                result.RollingWindow = rollingWindow
                                result.RollingPeriodsAnalyzed = len(rollingReturns)
                                result.RollingMin = rStats.Min
                                result.RollingMax = rStats.Max
                                result.RollingMedian = rStats.Median
                                result.RollingP25 = rStats.P25
                                result.RollingP75 = rStats.P75
                                result.CAGRMin = cStats.Min
                                result.CAGRMax = cStats.Max
                                result.CAGRMedian = cStats.Median
                        }
                }
        }

        return result, nil
}
