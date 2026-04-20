package models

import "time"

// Scheme represents a mutual fund tracked by the platform.
type Scheme struct {
        Code         string    `json:"scheme_code"`
        Name         string    `json:"scheme_name"`
        AMC          string    `json:"amc"`
        Category     string    `json:"category"`
        DiscoveredAt time.Time `json:"discovered_at"`
        LastSyncedAt *time.Time `json:"last_synced_at,omitempty"`
}

// NAVRecord holds a single daily NAV entry.
type NAVRecord struct {
        SchemeCode string    `json:"scheme_code"`
        Date       time.Time `json:"date"`
        NAV        float64   `json:"nav"`
}

// AnalyticsWindow represents a time window for analytics.
type AnalyticsWindow string

const (
        Window1Y  AnalyticsWindow = "1Y"
        Window3Y  AnalyticsWindow = "3Y"
        Window5Y  AnalyticsWindow = "5Y"
        Window10Y AnalyticsWindow = "10Y"
)

// WindowYears maps window strings to float years.
var WindowYears = map[AnalyticsWindow]float64{
        Window1Y:  1,
        Window3Y:  3,
        Window5Y:  5,
        Window10Y: 10,
}

// ValidWindows is the set of allowed windows.
var ValidWindows = map[string]AnalyticsWindow{
        "1Y":  Window1Y,
        "3Y":  Window3Y,
        "5Y":  Window5Y,
        "10Y": Window10Y,
}

// ReturnDistribution holds percentile stats for a metric.
type ReturnDistribution struct {
        Min    float64 `json:"min"`
        Max    float64 `json:"max"`
        Median float64 `json:"median"`
        P25    float64 `json:"p25"`
        P75    float64 `json:"p75"`
}

// CAGRDistribution holds CAGR percentile stats.
type CAGRDistribution struct {
        Min    float64 `json:"min"`
        Max    float64 `json:"max"`
        Median float64 `json:"median"`
}

// DataAvailability describes the NAV data coverage.
type DataAvailability struct {
        StartDate     string `json:"start_date"`
        EndDate       string `json:"end_date"`
        TotalDays     int    `json:"total_days"`
        NAVDataPoints int    `json:"nav_data_points"`
}

// Analytics holds pre-computed analytics for a fund+window.
type Analytics struct {
        SchemeCode             string             `json:"fund_code"`
        SchemeName             string             `json:"fund_name"`
        Category               string             `json:"category"`
        AMC                    string             `json:"amc"`
        Window                 AnalyticsWindow    `json:"window"`
        DataAvailability       DataAvailability   `json:"data_availability"`
        RollingPeriodsAnalyzed int                `json:"rolling_periods_analyzed"`
        RollingReturns         ReturnDistribution `json:"rolling_returns"`
        MaxDrawdown            float64            `json:"max_drawdown"`
        CAGR                   CAGRDistribution   `json:"cagr"`
        ComputedAt             time.Time          `json:"computed_at"`
}

// FundSummary is a lightweight summary for listing.
type FundSummary struct {
        Code         string     `json:"scheme_code"`
        Name         string     `json:"scheme_name"`
        AMC          string     `json:"amc"`
        Category     string     `json:"category"`
        LatestNAV    *float64   `json:"latest_nav,omitempty"`
        LatestNAVDate *string   `json:"latest_nav_date,omitempty"`
        LastSyncedAt *time.Time `json:"last_synced_at,omitempty"`
}

// FundDetail combines fund metadata with latest NAV.
type FundDetail struct {
        Code          string     `json:"scheme_code"`
        Name          string     `json:"scheme_name"`
        AMC           string     `json:"amc"`
        Category      string     `json:"category"`
        LatestNAV     *float64   `json:"latest_nav,omitempty"`
        LatestNAVDate *string    `json:"latest_nav_date,omitempty"`
        LastSyncedAt  *time.Time `json:"last_synced_at,omitempty"`
}

// RankedFund represents a fund in ranking results.
type RankedFund struct {
        Rank         int     `json:"rank"`
        FundCode     string  `json:"fund_code"`
        FundName     string  `json:"fund_name"`
        AMC          string  `json:"amc"`
        MedianReturn float64 `json:"median_return"`
        MaxDrawdown  float64 `json:"max_drawdown"`
        CurrentNAV   float64 `json:"current_nav"`
        LastUpdated  string  `json:"last_updated"`
}

// RankingResponse is the full ranking API response.
type RankingResponse struct {
        Category   string       `json:"category"`
        Window     string       `json:"window"`
        SortedBy   string       `json:"sorted_by"`
        TotalFunds int          `json:"total_funds"`
        Showing    int          `json:"showing"`
        Funds      []RankedFund `json:"funds"`
}

// PipelineStatus tracks sync pipeline state.
type PipelineStatus struct {
        Status           string            `json:"status"`
        SchemesTotal     int               `json:"schemes_total"`
        SchemesCompleted int               `json:"schemes_completed"`
        SchemesFailed    int               `json:"schemes_failed"`
        SchemesPending   int               `json:"schemes_pending"`
        LastRunAt        *time.Time        `json:"last_run_at,omitempty"`
        SchemeStatuses   []SchemeStatus    `json:"scheme_statuses"`
        RateLimiterStats RateLimiterStats  `json:"rate_limiter_stats"`
}

// SchemeStatus holds per-scheme pipeline state.
type SchemeStatus struct {
        Code         string     `json:"scheme_code"`
        Name         string     `json:"scheme_name"`
        Status       string     `json:"status"` // pending, in_progress, completed, failed
        LastSyncedAt *time.Time `json:"last_synced_at,omitempty"`
        Error        *string    `json:"error,omitempty"`
}

// RateLimiterStats are observability metrics for rate limiting.
type RateLimiterStats struct {
        RequestsLastSecond int `json:"requests_last_second"`
        RequestsLastMinute int `json:"requests_last_minute"`
        RequestsLastHour   int `json:"requests_last_hour"`
        LimitPerSecond     int `json:"limit_per_second"`
        LimitPerMinute     int `json:"limit_per_minute"`
        LimitPerHour       int `json:"limit_per_hour"`
}

// SyncTriggerResponse is returned when a sync is triggered.
type SyncTriggerResponse struct {
        Message     string `json:"message"`
        Status      string `json:"status"`
        TriggeredAt string `json:"triggered_at"`
}

// RangeReturnDistribution holds return stats for an arbitrary date-range query.
type RangeReturnDistribution struct {
        Min    float64 `json:"min"`
        Max    float64 `json:"max"`
        Median float64 `json:"median"`
        P25    float64 `json:"p25"`
        P75    float64 `json:"p75"`
}

// RangeCAGRDistribution holds CAGR stats for an arbitrary date-range query.
type RangeCAGRDistribution struct {
        Min    float64 `json:"min"`
        Max    float64 `json:"max"`
        Median float64 `json:"median"`
}

// RollingInRange holds rolling-window analytics computed within a date range.
type RollingInRange struct {
        Window                 string                  `json:"window"`
        RollingPeriodsAnalyzed int                     `json:"rolling_periods_analyzed"`
        RollingReturns         RangeReturnDistribution `json:"rolling_returns"`
        CAGR                   RangeCAGRDistribution   `json:"cagr"`
}

// RangeAnalytics is the on-the-fly analytics response for an arbitrary date range.
type RangeAnalytics struct {
        FundCode    string `json:"fund_code"`
        FundName    string `json:"fund_name"`
        Category    string `json:"category"`
        AMC         string `json:"amc"`
        RequestedFrom string `json:"requested_from"`
        RequestedTo   string `json:"requested_to"`
        DataAvailability struct {
                ActualFrom    string `json:"actual_from"`
                ActualTo      string `json:"actual_to"`
                CalendarDays  int    `json:"calendar_days"`
                NAVDataPoints int    `json:"nav_data_points"`
        } `json:"data_availability"`
        StartNAV            float64         `json:"start_nav"`
        EndNAV              float64         `json:"end_nav"`
        TotalReturnPct      float64         `json:"total_return_pct"`
        AnnualizedReturnPct float64         `json:"annualized_return_pct"`
        MaxDrawdownPct      float64         `json:"max_drawdown_pct"`
        Rolling             *RollingInRange `json:"rolling,omitempty"`
        ComputedAt          time.Time       `json:"computed_at"`
}
