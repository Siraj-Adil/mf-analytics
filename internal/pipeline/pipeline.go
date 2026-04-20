// Package pipeline orchestrates scheme discovery, NAV backfill,
// incremental sync, and analytics re-computation.
//
// Resumability: every scheme's sync state is tracked in the database.
// If the process crashes, a restart re-runs only incomplete/failed schemes.
//
// Rate-limit safety: all mfapi.in calls go through the three-tier limiter.
// The pipeline never calls the API directly.
package pipeline

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/mutual-fund-analytics/internal/analytics"
	"github.com/mutual-fund-analytics/internal/config"
	"github.com/mutual-fund-analytics/internal/db"
	"github.com/mutual-fund-analytics/internal/mfapi"
	"github.com/mutual-fund-analytics/internal/models"
)

// Pipeline orchestrates the full data ingestion and analytics lifecycle.
type Pipeline struct {
	cfg     *config.Config
	db      *db.DB
	client  *mfapi.Client
	engine  *analytics.Engine
	mu      sync.Mutex
	running bool
}

// New returns a Pipeline ready to run.
func New(cfg *config.Config, database *db.DB, client *mfapi.Client, engine *analytics.Engine) *Pipeline {
	return &Pipeline{
		cfg:    cfg,
		db:     database,
		client: client,
		engine: engine,
	}
}

// IsRunning reports whether the pipeline is currently active.
func (p *Pipeline) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// Run executes the full pipeline: discovery → backfill → analytics.
// It is safe to call concurrently — subsequent calls return immediately
// while an existing run is active.
func (p *Pipeline) Run(ctx context.Context, triggeredBy string) error {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return fmt.Errorf("pipeline already running")
	}
	p.running = true
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.running = false
		p.mu.Unlock()
	}()

	runID, err := p.db.StartSyncRun(triggeredBy)
	if err != nil {
		log.Printf("[pipeline] could not start sync run: %v", err)
	}

	log.Printf("[pipeline] starting run (triggered_by=%s)", triggeredBy)
	if err := p.run(ctx); err != nil {
		errStr := err.Error()
		p.db.EndSyncRun(runID, "failed", &errStr)
		return err
	}

	p.db.EndSyncRun(runID, "completed", nil)
	log.Printf("[pipeline] run complete")
	return nil
}

func (p *Pipeline) run(ctx context.Context) error {
	// Step 1: Discover schemes if we have fewer than expected.
	if err := p.discoverSchemes(ctx); err != nil {
		return fmt.Errorf("scheme discovery: %w", err)
	}

	// Step 2: Sync NAV data for all schemes.
	if err := p.syncAllSchemes(ctx); err != nil {
		return fmt.Errorf("nav sync: %w", err)
	}

	// Step 3: Re-compute analytics for all schemes.
	if err := p.computeAllAnalytics(ctx); err != nil {
		return fmt.Errorf("analytics: %w", err)
	}

	return nil
}

// DiscoverSchemes fetches the mfapi.in catalogue and filters to target schemes.
func (p *Pipeline) discoverSchemes(ctx context.Context) error {
	count, err := p.db.CountSchemes()
	if err != nil {
		return err
	}

	// If we already have schemes, skip discovery but still allow re-run.
	if count > 0 {
		log.Printf("[pipeline] %d schemes already in DB — skipping discovery", count)
		return nil
	}

	log.Printf("[pipeline] fetching full scheme catalogue from mfapi.in…")
	catalogue, err := p.client.FetchSchemeList(ctx)
	if err != nil {
		return fmt.Errorf("fetch catalogue: %w", err)
	}

	log.Printf("[pipeline] catalogue has %d schemes — filtering to target AMCs/categories", len(catalogue))

	candidates := p.filterCatalogue(catalogue)
	log.Printf("[pipeline] %d candidate schemes after name filter", len(candidates))

	// Fetch individual scheme details (rate-limited) to confirm metadata.
	confirmed := 0
	for _, item := range candidates {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		code := fmt.Sprintf("%d", item.SchemeCode)
		detail, err := p.client.FetchSchemeDetail(ctx, code)
		if err != nil {
			log.Printf("[pipeline] could not fetch detail for %s: %v", code, err)
			continue
		}

		if !p.matchesCriteria(detail.Meta.FundHouse, detail.Meta.SchemeCategory) {
			continue
		}

		category := normaliseCategory(detail.Meta.SchemeCategory)
		if err := p.db.UpsertScheme(code, detail.Meta.SchemeName, detail.Meta.FundHouse, category); err != nil {
			log.Printf("[pipeline] upsert scheme %s: %v", code, err)
			continue
		}
		// Initialise pipeline state as pending.
		if err := p.db.UpsertPipelineState(code, "pending", nil); err != nil {
			log.Printf("[pipeline] init pipeline state %s: %v", code, err)
		}

		confirmed++
		log.Printf("[pipeline] confirmed: [%s] %s (%s / %s)", code, detail.Meta.SchemeName, detail.Meta.FundHouse, category)
	}

	log.Printf("[pipeline] discovery complete: %d schemes confirmed", confirmed)
	if confirmed == 0 {
		log.Printf("[pipeline] WARNING: no schemes discovered — check AMC/category filters")
	}
	return nil
}

// filterCatalogue filters the catalogue by name keywords before making individual API calls.
func (p *Pipeline) filterCatalogue(catalogue []mfapi.SchemeListItem) []mfapi.SchemeListItem {
	amcKeywords := p.cfg.TargetAMCs
	catKeywords := p.cfg.TargetCategories
	required := p.cfg.TargetSubTypes

	var result []mfapi.SchemeListItem
	for _, item := range catalogue {
		name := item.SchemeName

		// Must contain at least one AMC keyword.
		hasAMC := false
		for _, kw := range amcKeywords {
			if strings.Contains(name, kw) {
				hasAMC = true
				break
			}
		}
		if !hasAMC {
			continue
		}

		// Must contain at least one category keyword.
		hasCat := false
		for _, kw := range catKeywords {
			if strings.Contains(name, kw) {
				hasCat = true
				break
			}
		}
		if !hasCat {
			continue
		}

		// Must contain all required terms.
		ok := true
		for _, kw := range required {
			if !strings.Contains(name, kw) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}

		result = append(result, item)
	}
	return result
}

// matchesCriteria verifies fund_house and scheme_category against targets.
func (p *Pipeline) matchesCriteria(fundHouse, schemeCategory string) bool {
	fundHouseLower := strings.ToLower(fundHouse)
	categoryLower := strings.ToLower(schemeCategory)

	amcMatched := false
	for _, amc := range p.cfg.TargetAMCs {
		if strings.Contains(fundHouseLower, strings.ToLower(amc)) {
			amcMatched = true
			break
		}
	}
	if !amcMatched {
		return false
	}

	catMatched := false
	for _, cat := range p.cfg.TargetCategories {
		if strings.Contains(categoryLower, strings.ToLower(cat)) {
			catMatched = true
			break
		}
	}
	return catMatched
}

// normaliseCategory produces a clean category label.
func normaliseCategory(raw string) string {
	lower := strings.ToLower(raw)
	if strings.Contains(lower, "mid cap") {
		return "Equity: Mid Cap"
	}
	if strings.Contains(lower, "small cap") {
		return "Equity: Small Cap"
	}
	return raw
}

// syncAllSchemes fetches full NAV history for all schemes, resumably.
func (p *Pipeline) syncAllSchemes(ctx context.Context) error {
	schemes, err := p.db.ListSchemes("", "")
	if err != nil {
		return err
	}
	if len(schemes) == 0 {
		log.Printf("[pipeline] no schemes to sync")
		return nil
	}

	states, err := p.db.GetPipelineStates()
	if err != nil {
		return err
	}
	stateMap := make(map[string]string, len(states))
	for _, s := range states {
		stateMap[s.SchemeCode] = s.Status
	}

	log.Printf("[pipeline] syncing NAV data for %d schemes", len(schemes))

	for _, scheme := range schemes {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		status := stateMap[scheme.Code]
		if status == "completed" {
			// Incremental: only fetch new data since last sync.
			if err := p.incrementalSync(ctx, scheme.Code); err != nil {
				log.Printf("[pipeline] incremental sync %s: %v", scheme.Code, err)
			}
			continue
		}

		// Full backfill for new or failed schemes.
		if err := p.backfillScheme(ctx, scheme.Code); err != nil {
			errStr := err.Error()
			p.db.UpsertPipelineState(scheme.Code, "failed", &errStr)
			log.Printf("[pipeline] backfill %s failed: %v", scheme.Code, err)
			continue
		}

		p.db.MarkPipelineSynced(scheme.Code)
		p.db.MarkSchemeSynced(scheme.Code)
	}

	return nil
}

// backfillScheme fetches full NAV history for a scheme.
func (p *Pipeline) backfillScheme(ctx context.Context, code string) error {
	p.db.UpsertPipelineState(code, "in_progress", nil)

	log.Printf("[pipeline] backfilling %s…", code)
	detail, err := p.client.FetchSchemeDetail(ctx, code)
	if err != nil {
		return err
	}

	rows, skipped := parseNAVEntries(code, detail.Data)
	if skipped > 0 {
		log.Printf("[pipeline] %s: skipped %d invalid NAV entries", code, skipped)
	}
	if len(rows) == 0 {
		return fmt.Errorf("no valid NAV data returned for scheme %s", code)
	}

	if err := p.db.BulkUpsertNAV(rows); err != nil {
		return fmt.Errorf("store NAV: %w", err)
	}

	log.Printf("[pipeline] %s: stored %d NAV records (oldest=%s, latest=%s)",
		code, len(rows), rows[0].Date, rows[len(rows)-1].Date)
	return nil
}

// incrementalSync fetches only new NAV data since last sync.
func (p *Pipeline) incrementalSync(ctx context.Context, code string) error {
	lastDate, err := p.db.GetLatestNAVDate(code)
	if err != nil {
		return err
	}

	detail, err := p.client.FetchSchemeDetail(ctx, code)
	if err != nil {
		return err
	}

	var newEntries []mfapi.NAVEntry
	for _, e := range detail.Data {
		t, err := mfapi.ParseNAVDate(e.Date)
		if err != nil {
			continue
		}
		entryDate := t.Format("2006-01-02")
		if lastDate == "" || entryDate > lastDate {
			newEntries = append(newEntries, e)
		}
	}

	if len(newEntries) == 0 {
		log.Printf("[pipeline] %s: already up to date", code)
		return nil
	}

	rows, _ := parseNAVEntries(code, newEntries)
	if err := p.db.BulkUpsertNAV(rows); err != nil {
		return err
	}

	p.db.MarkSchemeSynced(code)
	log.Printf("[pipeline] %s: added %d new NAV records", code, len(rows))
	return nil
}

// computeAllAnalytics re-computes analytics for all schemes with data.
func (p *Pipeline) computeAllAnalytics(ctx context.Context) error {
	schemes, err := p.db.ListSchemes("", "")
	if err != nil {
		return err
	}

	for _, scheme := range schemes {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		count, _ := p.db.CountNAVRecords(scheme.Code)
		if count < 20 {
			log.Printf("[pipeline] %s: only %d NAV records — skipping analytics", scheme.Code, count)
			continue
		}
		if err := p.engine.ComputeAll(scheme.Code); err != nil {
			log.Printf("[pipeline] analytics for %s: %v", scheme.Code, err)
		}
	}
	return nil
}

// GetStatus assembles a PipelineStatus for the status endpoint.
func (p *Pipeline) GetStatus() (*models.PipelineStatus, error) {
	states, err := p.db.GetPipelineStates()
	if err != nil {
		return nil, err
	}

	schemeSummaries := make([]models.SchemeStatus, 0, len(states))
	var completed, failed, pending int

	for _, s := range states {
		var lastSynced *time.Time
		if s.LastSyncedAt != nil {
			t, err := time.Parse(time.RFC3339, *s.LastSyncedAt)
			if err == nil {
				lastSynced = &t
			}
		}

		schemeSummaries = append(schemeSummaries, models.SchemeStatus{
			Code:         s.SchemeCode,
			Status:       s.Status,
			LastSyncedAt: lastSynced,
			Error:        s.Error,
		})

		switch s.Status {
		case "completed":
			completed++
		case "failed":
			failed++
		default:
			pending++
		}
	}

	status := "idle"
	if p.IsRunning() {
		status = "running"
	}

	return &models.PipelineStatus{
		Status:           status,
		SchemesTotal:     len(states),
		SchemesCompleted: completed,
		SchemesFailed:    failed,
		SchemesPending:   pending,
		SchemeStatuses:   schemeSummaries,
	}, nil
}

// parseNAVEntries converts API NAVEntry slices to db.NAVRow slices.
func parseNAVEntries(schemeCode string, entries []mfapi.NAVEntry) ([]db.NAVRow, int) {
	rows := make([]db.NAVRow, 0, len(entries))
	skipped := 0
	for _, e := range entries {
		t, err := mfapi.ParseNAVDate(e.Date)
		if err != nil {
			skipped++
			continue
		}
		nav, err := mfapi.ParseNAV(e.NAV)
		if err != nil {
			skipped++
			continue
		}
		rows = append(rows, db.NAVRow{
			SchemeCode: schemeCode,
			Date:       t.Format("2006-01-02"),
			NAV:        nav,
		})
	}
	return rows, skipped
}
