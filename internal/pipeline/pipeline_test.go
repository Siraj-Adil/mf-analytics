package pipeline_test

import (
	"os"
	"testing"

	"github.com/mutual-fund-analytics/internal/db"
)

// setupTestDB creates a temporary SQLite database for pipeline tests.
func setupTestDB(t *testing.T) (*db.DB, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "mf_pipeline_test_*.db")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	f.Close()

	database, err := db.New(f.Name())
	if err != nil {
		os.Remove(f.Name())
		t.Fatalf("open db: %v", err)
	}

	return database, func() {
		database.Close()
		os.Remove(f.Name())
	}
}

// TestPipelineStateTracking verifies that scheme state transitions
// are persisted correctly and can be used for resumability.
func TestPipelineStateTracking(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	code := "TEST_SCHEME"
	if err := database.UpsertScheme(code, "Test Fund", "Test AMC", "Equity: Mid Cap"); err != nil {
		t.Fatalf("upsert scheme: %v", err)
	}

	// Mark as pending.
	if err := database.UpsertPipelineState(code, "pending", nil); err != nil {
		t.Fatalf("set pending: %v", err)
	}

	// Mark as in_progress.
	if err := database.UpsertPipelineState(code, "in_progress", nil); err != nil {
		t.Fatalf("set in_progress: %v", err)
	}

	// Mark as failed with an error.
	errMsg := "connection timeout"
	if err := database.UpsertPipelineState(code, "failed", &errMsg); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	// Verify the state was persisted correctly.
	states, err := database.GetPipelineStates()
	if err != nil {
		t.Fatalf("get states: %v", err)
	}

	if len(states) != 1 {
		t.Fatalf("expected 1 state, got %d", len(states))
	}
	if states[0].Status != "failed" {
		t.Errorf("expected status 'failed', got %q", states[0].Status)
	}
	if states[0].Error == nil || *states[0].Error != errMsg {
		t.Errorf("expected error %q, got %v", errMsg, states[0].Error)
	}
}

// TestResumability verifies that completed schemes are not re-processed
// after a simulated restart.
func TestResumability(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	// Insert two schemes.
	for _, code := range []string{"S1", "S2"} {
		database.UpsertScheme(code, "Fund "+code, "Test AMC", "Equity: Mid Cap")
	}

	// Simulate: S1 completed, S2 failed.
	database.UpsertPipelineState("S1", "completed", nil)
	database.MarkPipelineSynced("S1")
	errMsg := "timeout"
	database.UpsertPipelineState("S2", "failed", &errMsg)

	// A restart would load states and skip completed ones.
	states, err := database.GetPipelineStates()
	if err != nil {
		t.Fatalf("get states: %v", err)
	}

	completedCount := 0
	retryCount := 0
	for _, s := range states {
		switch s.Status {
		case "completed":
			completedCount++
		default: // pending, failed, in_progress should all be retried
			retryCount++
		}
	}

	if completedCount != 1 {
		t.Errorf("expected 1 completed scheme, got %d", completedCount)
	}
	if retryCount != 1 {
		t.Errorf("expected 1 scheme to retry, got %d", retryCount)
	}
}

// TestBulkNAVUpsert_Idempotency verifies that reinserting the same NAV
// records does not create duplicates.
func TestBulkNAVUpsert_Idempotency(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	database.UpsertScheme("IDMP", "Idempotency Fund", "Test AMC", "Equity: Mid Cap")

	rows := []db.NAVRow{
		{SchemeCode: "IDMP", Date: "2024-01-01", NAV: 100.0},
		{SchemeCode: "IDMP", Date: "2024-01-02", NAV: 101.0},
	}

	// Insert twice.
	if err := database.BulkUpsertNAV(rows); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	// Upsert with updated NAV for second date (simulating API correction).
	rows[1].NAV = 101.5
	if err := database.BulkUpsertNAV(rows); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	count, err := database.CountNAVRecords("IDMP")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 records after idempotent upserts, got %d", count)
	}

	// Verify the updated NAV was stored.
	latest, err := database.GetLatestNAV("IDMP")
	if err != nil || latest == nil {
		t.Fatalf("get latest nav: %v", err)
	}
	if latest.NAV != 101.5 {
		t.Errorf("expected NAV 101.5, got %.2f", latest.NAV)
	}
}

// TestIncrementalSyncDelta verifies that only newer NAV records are
// returned when checking for incremental updates.
func TestIncrementalSyncDelta(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	database.UpsertScheme("INCR", "Incremental Fund", "Test AMC", "Equity: Mid Cap")

	existing := []db.NAVRow{
		{SchemeCode: "INCR", Date: "2024-01-01", NAV: 100.0},
		{SchemeCode: "INCR", Date: "2024-01-02", NAV: 101.0},
	}
	database.BulkUpsertNAV(existing)

	latestDate, err := database.GetLatestNAVDate("INCR")
	if err != nil {
		t.Fatalf("get latest date: %v", err)
	}
	if latestDate != "2024-01-02" {
		t.Errorf("expected latest date 2024-01-02, got %s", latestDate)
	}

	// New entries from API (includes existing + new).
	allEntries := []db.NAVRow{
		{SchemeCode: "INCR", Date: "2024-01-01", NAV: 100.0},
		{SchemeCode: "INCR", Date: "2024-01-02", NAV: 101.0},
		{SchemeCode: "INCR", Date: "2024-01-03", NAV: 102.0}, // new
		{SchemeCode: "INCR", Date: "2024-01-04", NAV: 103.0}, // new
	}

	// Simulate filtering: only entries newer than latestDate.
	var newEntries []db.NAVRow
	for _, e := range allEntries {
		if e.Date > latestDate {
			newEntries = append(newEntries, e)
		}
	}

	if len(newEntries) != 2 {
		t.Errorf("expected 2 new entries, got %d", len(newEntries))
	}
}

// TestSyncRunAuditLog verifies that sync runs are recorded correctly.
func TestSyncRunAuditLog(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	id, err := database.StartSyncRun("test")
	if err != nil {
		t.Fatalf("start sync run: %v", err)
	}
	if id <= 0 {
		t.Errorf("expected positive run ID, got %d", id)
	}

	if err := database.EndSyncRun(id, "completed", nil); err != nil {
		t.Fatalf("end sync run: %v", err)
	}
}

// TestNAVDateOrder verifies GetNAVHistory returns records in ascending date order.
func TestNAVDateOrder(t *testing.T) {
	database, cleanup := setupTestDB(t)
	defer cleanup()

	database.UpsertScheme("ORD", "Order Fund", "Test AMC", "Equity: Mid Cap")

	// Insert out of order.
	rows := []db.NAVRow{
		{SchemeCode: "ORD", Date: "2024-01-03", NAV: 103.0},
		{SchemeCode: "ORD", Date: "2024-01-01", NAV: 101.0},
		{SchemeCode: "ORD", Date: "2024-01-02", NAV: 102.0},
	}
	database.BulkUpsertNAV(rows)

	history, err := database.GetNAVHistory("ORD")
	if err != nil {
		t.Fatalf("get history: %v", err)
	}

	for i := 1; i < len(history); i++ {
		if history[i].Date < history[i-1].Date {
			t.Errorf("NAV history not in ascending order: %s before %s",
				history[i-1].Date, history[i].Date)
		}
	}
}
