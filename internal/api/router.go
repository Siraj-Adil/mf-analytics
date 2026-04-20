package api

import (
        "net/http"
        "time"

        "github.com/go-chi/chi/v5"
        "github.com/go-chi/chi/v5/middleware"
)

// NewRouter builds and returns the application's HTTP router.
func NewRouter(h *Handler) http.Handler {
        r := chi.NewRouter()

        // Core middleware.
        r.Use(middleware.RequestID)
        r.Use(middleware.RealIP)
        r.Use(middleware.Logger)
        r.Use(middleware.Recoverer)
        r.Use(middleware.Timeout(30 * time.Second))

        // CORS (useful for browser-based tooling).
        r.Use(corsMiddleware)

        // Health.
        r.Get("/healthz", h.Health)

        // Fund endpoints.
        r.Get("/funds", h.ListFunds)
        r.Get("/funds/rank", h.RankFunds)     // must be before /funds/{code}
        r.Get("/funds/{code}", h.GetFund)
        r.Get("/funds/{code}/analytics", h.GetAnalytics)
        r.Get("/funds/{code}/analytics/range", h.GetRangeAnalytics)

        // Sync endpoints.
        r.Post("/sync/trigger", h.TriggerSync)
        r.Get("/sync/status", h.GetSyncStatus)

        return r
}

// corsMiddleware adds permissive CORS headers for development convenience.
func corsMiddleware(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
                w.Header().Set("Access-Control-Allow-Origin", "*")
                w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
                if r.Method == http.MethodOptions {
                        w.WriteHeader(http.StatusNoContent)
                        return
                }
                next.ServeHTTP(w, r)
        })
}
