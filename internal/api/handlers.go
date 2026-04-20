// Package api implements the HTTP request handlers for the analytics service.
package api

import (
        "encoding/json"
        "log"
        "net/http"
        "sort"
        "strconv"
        "strings"
        "time"

        "github.com/go-chi/chi/v5"
        "github.com/mutual-fund-analytics/internal/analytics"
        "github.com/mutual-fund-analytics/internal/db"
        "github.com/mutual-fund-analytics/internal/models"
        "github.com/mutual-fund-analytics/internal/pipeline"
        "github.com/mutual-fund-analytics/internal/ratelimiter"
)

// Handler holds all handler dependencies.
type Handler struct {
        db       *db.DB
        pipeline *pipeline.Pipeline
        engine   *analytics.Engine
        limiter  *ratelimiter.Limiter
}

// NewHandler returns an initialised Handler.
func NewHandler(database *db.DB, pipe *pipeline.Pipeline, eng *analytics.Engine, lim *ratelimiter.Limiter) *Handler {
        return &Handler{
                db:       database,
                pipeline: pipe,
                engine:   eng,
                limiter:  lim,
        }
}

// ---- /funds ----------------------------------------------------------------

// ListFunds handles GET /funds with optional ?amc= and ?category= filters.
func (h *Handler) ListFunds(w http.ResponseWriter, r *http.Request) {
        amc := r.URL.Query().Get("amc")
        category := r.URL.Query().Get("category")

        schemes, err := h.db.ListSchemes(amc, category)
        if err != nil {
                writeError(w, http.StatusInternalServerError, "database error: "+err.Error())
                return
        }

        type response struct {
                Total int                    `json:"total"`
                Funds []models.FundSummary   `json:"funds"`
        }

        funds := make([]models.FundSummary, 0, len(schemes))
        for _, s := range schemes {
                nav, _ := h.db.GetLatestNAV(s.Code)
                fund := models.FundSummary{
                        Code:     s.Code,
                        Name:     s.Name,
                        AMC:      s.AMC,
                        Category: s.Category,
                }
                if nav != nil {
                        fund.LatestNAV = &nav.NAV
                        fund.LatestNAVDate = &nav.Date
                }
                funds = append(funds, fund)
        }

        writeJSON(w, http.StatusOK, response{Total: len(funds), Funds: funds})
}

// GetFund handles GET /funds/{code}.
func (h *Handler) GetFund(w http.ResponseWriter, r *http.Request) {
        code := chi.URLParam(r, "code")

        scheme, err := h.db.GetScheme(code)
        if err != nil {
                writeError(w, http.StatusInternalServerError, "database error: "+err.Error())
                return
        }
        if scheme == nil {
                writeError(w, http.StatusNotFound, "fund not found: "+code)
                return
        }

        nav, _ := h.db.GetLatestNAV(code)
        detail := models.FundDetail{
                Code:     scheme.Code,
                Name:     scheme.Name,
                AMC:      scheme.AMC,
                Category: scheme.Category,
        }
        if nav != nil {
                detail.LatestNAV = &nav.NAV
                detail.LatestNAVDate = &nav.Date
        }

        writeJSON(w, http.StatusOK, detail)
}

// ---- /funds/{code}/analytics -----------------------------------------------

// GetAnalytics handles GET /funds/{code}/analytics?window=3Y.
func (h *Handler) GetAnalytics(w http.ResponseWriter, r *http.Request) {
        code := chi.URLParam(r, "code")
        windowStr := r.URL.Query().Get("window")

        if windowStr == "" {
                writeError(w, http.StatusBadRequest, "window query parameter is required (1Y|3Y|5Y|10Y)")
                return
        }
        win, ok := models.ValidWindows[windowStr]
        if !ok {
                writeError(w, http.StatusBadRequest, "invalid window; must be one of 1Y, 3Y, 5Y, 10Y")
                return
        }

        scheme, err := h.db.GetScheme(code)
        if err != nil || scheme == nil {
                writeError(w, http.StatusNotFound, "fund not found: "+code)
                return
        }

        row, err := h.db.GetAnalytics(code, string(win))
        if err != nil {
                writeError(w, http.StatusInternalServerError, "database error: "+err.Error())
                return
        }
        if row == nil {
                writeError(w, http.StatusNotFound,
                        "analytics not yet computed for "+code+"/"+windowStr+"; trigger a sync first")
                return
        }

        computed, _ := time.Parse(time.RFC3339, row.ComputedAt)

        resp := models.Analytics{
                SchemeCode: code,
                SchemeName: scheme.Name,
                Category:   scheme.Category,
                AMC:        scheme.AMC,
                Window:     win,
                DataAvailability: models.DataAvailability{
                        StartDate:     row.StartDate,
                        EndDate:       row.EndDate,
                        TotalDays:     row.TotalDays,
                        NAVDataPoints: row.NAVDataPoints,
                },
                RollingPeriodsAnalyzed: row.RollingPeriodsAnalyzed,
                RollingReturns: models.ReturnDistribution{
                        Min:    round2(row.RollingMin),
                        Max:    round2(row.RollingMax),
                        Median: round2(row.RollingMedian),
                        P25:    round2(row.RollingP25),
                        P75:    round2(row.RollingP75),
                },
                MaxDrawdown: round2(row.MaxDrawdown),
                CAGR: models.CAGRDistribution{
                        Min:    round2(row.CAGRMin),
                        Max:    round2(row.CAGRMax),
                        Median: round2(row.CAGRMedian),
                },
                ComputedAt: computed,
        }

        writeJSON(w, http.StatusOK, resp)
}

// ---- /funds/{code}/analytics/range ----------------------------------------

// GetRangeAnalytics handles GET /funds/{code}/analytics/range?from=YYYY-MM-DD&to=YYYY-MM-DD[&rolling_window=1Y]
func (h *Handler) GetRangeAnalytics(w http.ResponseWriter, r *http.Request) {
        code := chi.URLParam(r, "code")
        q := r.URL.Query()

        fromStr := strings.TrimSpace(q.Get("from"))
        toStr := strings.TrimSpace(q.Get("to"))
        rollingWindow := strings.ToUpper(strings.TrimSpace(q.Get("rolling_window")))

        if fromStr == "" || toStr == "" {
                writeError(w, http.StatusBadRequest, "both 'from' and 'to' query parameters are required (YYYY-MM-DD)")
                return
        }

        fromDate, err := time.Parse("2006-01-02", fromStr)
        if err != nil {
                writeError(w, http.StatusBadRequest, "invalid 'from' date: must be YYYY-MM-DD")
                return
        }
        toDate, err := time.Parse("2006-01-02", toStr)
        if err != nil {
                writeError(w, http.StatusBadRequest, "invalid 'to' date: must be YYYY-MM-DD")
                return
        }
        if !toDate.After(fromDate) {
                writeError(w, http.StatusBadRequest, "'to' must be strictly after 'from'")
                return
        }
        if rollingWindow != "" {
                if _, ok := models.ValidWindows[rollingWindow]; !ok {
                        writeError(w, http.StatusBadRequest, "invalid rolling_window; must be one of 1Y, 3Y, 5Y, 10Y")
                        return
                }
        }

        scheme, err := h.db.GetScheme(code)
        if err != nil || scheme == nil {
                writeError(w, http.StatusNotFound, "fund not found: "+code)
                return
        }

        result, err := h.engine.ComputeForDateRange(code, fromStr, toStr, rollingWindow)
        if err != nil {
                writeError(w, http.StatusUnprocessableEntity, err.Error())
                return
        }

        resp := models.RangeAnalytics{
                FundCode:      code,
                FundName:      scheme.Name,
                Category:      scheme.Category,
                AMC:           scheme.AMC,
                RequestedFrom: fromStr,
                RequestedTo:   toStr,
                StartNAV:      round2(result.StartNAV),
                EndNAV:        round2(result.EndNAV),
                TotalReturnPct:      round2(result.TotalReturnPct),
                AnnualizedReturnPct: round2(result.AnnualizedReturnPct),
                MaxDrawdownPct:      round2(result.MaxDrawdownPct),
                ComputedAt:    time.Now().UTC(),
        }
        resp.DataAvailability.ActualFrom = result.ActualFrom
        resp.DataAvailability.ActualTo = result.ActualTo
        resp.DataAvailability.CalendarDays = result.CalendarDays
        resp.DataAvailability.NAVDataPoints = result.NAVDataPoints

        if result.HasRolling {
                resp.Rolling = &models.RollingInRange{
                        Window:                 result.RollingWindow,
                        RollingPeriodsAnalyzed: result.RollingPeriodsAnalyzed,
                        RollingReturns: models.RangeReturnDistribution{
                                Min:    round2(result.RollingMin),
                                Max:    round2(result.RollingMax),
                                Median: round2(result.RollingMedian),
                                P25:    round2(result.RollingP25),
                                P75:    round2(result.RollingP75),
                        },
                        CAGR: models.RangeCAGRDistribution{
                                Min:    round2(result.CAGRMin),
                                Max:    round2(result.CAGRMax),
                                Median: round2(result.CAGRMedian),
                        },
                }
        }

        writeJSON(w, http.StatusOK, resp)
}

// ---- /funds/rank -----------------------------------------------------------

// RankFunds handles GET /funds/rank with ranking and filtering.
func (h *Handler) RankFunds(w http.ResponseWriter, r *http.Request) {
        q := r.URL.Query()

        category := q.Get("category")
        if category == "" {
                writeError(w, http.StatusBadRequest, "category query parameter is required")
                return
        }

        windowStr := q.Get("window")
        if windowStr == "" {
                writeError(w, http.StatusBadRequest, "window query parameter is required (1Y|3Y|5Y|10Y)")
                return
        }
        if _, ok := models.ValidWindows[windowStr]; !ok {
                writeError(w, http.StatusBadRequest, "invalid window; must be one of 1Y, 3Y, 5Y, 10Y")
                return
        }

        sortBy := q.Get("sort_by")
        if sortBy == "" {
                sortBy = "median_return"
        }
        if sortBy != "median_return" && sortBy != "max_drawdown" {
                writeError(w, http.StatusBadRequest, "sort_by must be median_return or max_drawdown")
                return
        }

        limitStr := q.Get("limit")
        limit := 5
        if limitStr != "" {
                if n, err := strconv.Atoi(limitStr); err == nil && n > 0 {
                        limit = n
                }
        }

        rows, err := h.db.GetAnalyticsByCategory(category, windowStr)
        if err != nil {
                writeError(w, http.StatusInternalServerError, "database error: "+err.Error())
                return
        }

        // Sort by chosen metric.
        switch sortBy {
        case "median_return":
                sort.Slice(rows, func(i, j int) bool {
                        return rows[i].RollingMedian > rows[j].RollingMedian // descending
                })
        case "max_drawdown":
                sort.Slice(rows, func(i, j int) bool {
                        return rows[i].MaxDrawdown > rows[j].MaxDrawdown // less negative = better
                })
        }

        total := len(rows)
        if limit < total {
                rows = rows[:limit]
        }

        funds := make([]models.RankedFund, 0, len(rows))
        for i, r := range rows {
                scheme, _ := h.db.GetScheme(r.SchemeCode)
                nav, _ := h.db.GetLatestNAV(r.SchemeCode)

                fund := models.RankedFund{
                        Rank:         i + 1,
                        FundCode:     r.SchemeCode,
                        MedianReturn: round2(r.RollingMedian),
                        MaxDrawdown:  round2(r.MaxDrawdown),
                }
                if scheme != nil {
                        fund.FundName = scheme.Name
                        fund.AMC = scheme.AMC
                }
                if nav != nil {
                        fund.CurrentNAV = nav.NAV
                        fund.LastUpdated = nav.Date
                }
                funds = append(funds, fund)
        }

        resp := models.RankingResponse{
                Category:   category,
                Window:     windowStr,
                SortedBy:   sortBy,
                TotalFunds: total,
                Showing:    len(funds),
                Funds:      funds,
        }

        writeJSON(w, http.StatusOK, resp)
}

// ---- /sync -----------------------------------------------------------------

// TriggerSync handles POST /sync/trigger.
func (h *Handler) TriggerSync(w http.ResponseWriter, r *http.Request) {
        if h.pipeline.IsRunning() {
                writeError(w, http.StatusConflict, "sync pipeline is already running")
                return
        }

        go func() {
                ctx := r.Context()
                if err := h.pipeline.Run(ctx, "manual"); err != nil {
                        log.Printf("[sync] triggered sync failed: %v", err)
                }
        }()

        writeJSON(w, http.StatusAccepted, models.SyncTriggerResponse{
                Message:     "sync pipeline triggered",
                Status:      "started",
                TriggeredAt: time.Now().UTC().Format(time.RFC3339),
        })
}

// GetSyncStatus handles GET /sync/status.
func (h *Handler) GetSyncStatus(w http.ResponseWriter, r *http.Request) {
        status, err := h.pipeline.GetStatus()
        if err != nil {
                writeError(w, http.StatusInternalServerError, "could not get status: "+err.Error())
                return
        }

        // Enrich scheme names from DB.
        for i := range status.SchemeStatuses {
                scheme, _ := h.db.GetScheme(status.SchemeStatuses[i].Code)
                if scheme != nil {
                        status.SchemeStatuses[i].Name = scheme.Name
                }
        }

        // Add rate-limiter stats.
        sec, min, hour := h.limiter.Stats()
        limitSec, limitMin, limitHour := h.limiter.Limits()
        status.RateLimiterStats = models.RateLimiterStats{
                RequestsLastSecond: sec,
                RequestsLastMinute: min,
                RequestsLastHour:   hour,
                LimitPerSecond:     limitSec,
                LimitPerMinute:     limitMin,
                LimitPerHour:       limitHour,
        }

        writeJSON(w, http.StatusOK, status)
}

// ---- Healthcheck -----------------------------------------------------------

// Health handles GET /healthz.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
        writeJSON(w, http.StatusOK, map[string]string{
                "status":  "ok",
                "service": "mutual-fund-analytics",
                "time":    time.Now().UTC().Format(time.RFC3339),
        })
}

// ---- Helpers ---------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(status)
        if err := json.NewEncoder(w).Encode(v); err != nil {
                log.Printf("[api] encode response: %v", err)
        }
}

func writeError(w http.ResponseWriter, status int, msg string) {
        writeJSON(w, status, map[string]string{"error": msg})
}

func round2(v float64) float64 {
        // Round to 2 decimal places for clean API responses.
        return float64(int(v*100+0.5)) / 100
}

// normaliseWindowParam returns a canonical window string or empty if invalid.
func normaliseWindowParam(s string) string {
        return strings.ToUpper(strings.TrimSpace(s))
}
