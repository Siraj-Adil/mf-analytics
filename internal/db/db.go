// Package db handles all database interactions using SQLite via modernc.org/sqlite.
package db

import (
        "database/sql"
        "fmt"
        "log"
        "time"

        _ "modernc.org/sqlite"
)

// DB wraps the sql.DB connection and exposes all query methods.
type DB struct {
        conn *sql.DB
}

// New opens (or creates) a SQLite database at the given path and runs migrations.
func New(path string) (*DB, error) {
        conn, err := sql.Open("sqlite", path+"?_journal_mode=WAL&_foreign_keys=on&_busy_timeout=5000")
        if err != nil {
                return nil, fmt.Errorf("open sqlite: %w", err)
        }

        // Keep a small connection pool to avoid locking issues.
        conn.SetMaxOpenConns(1)
        conn.SetMaxIdleConns(1)
        conn.SetConnMaxLifetime(30 * time.Minute)

        db := &DB{conn: conn}
        if err := db.migrate(); err != nil {
                conn.Close()
                return nil, fmt.Errorf("migrate: %w", err)
        }

        log.Printf("[db] opened database at %s", path)
        return db, nil
}

// Close closes the database connection.
func (db *DB) Close() error {
        return db.conn.Close()
}

// Conn returns the underlying *sql.DB for direct queries.
func (db *DB) Conn() *sql.DB {
        return db.conn
}

// migrate runs all schema migrations idempotently.
func (db *DB) migrate() error {
        stmts := []string{
                `CREATE TABLE IF NOT EXISTS schemes (
                        code          TEXT PRIMARY KEY,
                        name          TEXT NOT NULL,
                        amc           TEXT NOT NULL,
                        category      TEXT NOT NULL,
                        discovered_at TEXT NOT NULL,
                        last_synced_at TEXT
                )`,

                `CREATE TABLE IF NOT EXISTS nav_data (
                        scheme_code TEXT NOT NULL,
                        date        TEXT NOT NULL,
                        nav         REAL NOT NULL,
                        PRIMARY KEY (scheme_code, date),
                        FOREIGN KEY (scheme_code) REFERENCES schemes(code)
                )`,

                `CREATE INDEX IF NOT EXISTS idx_nav_scheme_date
                        ON nav_data(scheme_code, date)`,

                `CREATE TABLE IF NOT EXISTS analytics (
                        scheme_code              TEXT NOT NULL,
                        window                   TEXT NOT NULL,
                        rolling_min              REAL,
                        rolling_max              REAL,
                        rolling_median           REAL,
                        rolling_p25              REAL,
                        rolling_p75              REAL,
                        max_drawdown             REAL,
                        cagr_min                 REAL,
                        cagr_max                 REAL,
                        cagr_median              REAL,
                        rolling_periods_analyzed INTEGER,
                        start_date               TEXT,
                        end_date                 TEXT,
                        total_days               INTEGER,
                        nav_data_points          INTEGER,
                        computed_at              TEXT,
                        PRIMARY KEY (scheme_code, window),
                        FOREIGN KEY (scheme_code) REFERENCES schemes(code)
                )`,

                `CREATE TABLE IF NOT EXISTS pipeline_state (
                        scheme_code   TEXT PRIMARY KEY,
                        status        TEXT NOT NULL,
                        last_synced_at TEXT,
                        error         TEXT,
                        updated_at    TEXT NOT NULL
                )`,

                // Stores serialised rate-limiter request timestamps for crash recovery.
                `CREATE TABLE IF NOT EXISTS rate_limiter_state (
                        window_type   TEXT PRIMARY KEY,
                        request_times TEXT NOT NULL,
                        updated_at    TEXT NOT NULL
                )`,

                // Lightweight sync-run audit log.
                `CREATE TABLE IF NOT EXISTS sync_runs (
                        id         INTEGER PRIMARY KEY AUTOINCREMENT,
                        started_at TEXT NOT NULL,
                        ended_at   TEXT,
                        triggered_by TEXT NOT NULL DEFAULT 'auto',
                        status     TEXT NOT NULL DEFAULT 'running',
                        error      TEXT
                )`,
        }

        for _, stmt := range stmts {
                if _, err := db.conn.Exec(stmt); err != nil {
                        return fmt.Errorf("exec migration %q: %w", stmt[:40], err)
                }
        }
        return nil
}

// ---- Scheme queries --------------------------------------------------------

// UpsertScheme inserts or replaces a scheme record.
func (db *DB) UpsertScheme(code, name, amc, category string) error {
        _, err := db.conn.Exec(`
                INSERT INTO schemes (code, name, amc, category, discovered_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                        name=excluded.name,
                        amc=excluded.amc,
                        category=excluded.category`,
                code, name, amc, category, time.Now().UTC().Format(time.RFC3339))
        return err
}

// GetScheme returns a single scheme by code.
func (db *DB) GetScheme(code string) (*SchemeRow, error) {
        row := db.conn.QueryRow(`
                SELECT code, name, amc, category, discovered_at, last_synced_at
                FROM schemes WHERE code = ?`, code)
        return scanScheme(row)
}

// ListSchemes returns schemes filtered by optional AMC and category.
func (db *DB) ListSchemes(amc, category string) ([]*SchemeRow, error) {
        query := `SELECT code, name, amc, category, discovered_at, last_synced_at FROM schemes WHERE 1=1`
        args := []interface{}{}
        if amc != "" {
                query += ` AND amc LIKE ?`
                args = append(args, "%"+amc+"%")
        }
        if category != "" {
                query += ` AND category LIKE ?`
                args = append(args, "%"+category+"%")
        }
        query += ` ORDER BY amc, name`

        rows, err := db.conn.Query(query, args...)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var result []*SchemeRow
        for rows.Next() {
                s, err := scanScheme(rows)
                if err != nil {
                        return nil, err
                }
                result = append(result, s)
        }
        return result, rows.Err()
}

// CountSchemes returns total number of stored schemes.
func (db *DB) CountSchemes() (int, error) {
        var n int
        err := db.conn.QueryRow(`SELECT COUNT(*) FROM schemes`).Scan(&n)
        return n, err
}

// MarkSchemeSynced updates last_synced_at for a scheme.
func (db *DB) MarkSchemeSynced(code string) error {
        _, err := db.conn.Exec(`UPDATE schemes SET last_synced_at=? WHERE code=?`,
                time.Now().UTC().Format(time.RFC3339), code)
        return err
}

// ---- NAV queries -----------------------------------------------------------

// BulkUpsertNAV inserts many NAV records efficiently within a transaction.
func (db *DB) BulkUpsertNAV(records []NAVRow) error {
        if len(records) == 0 {
                return nil
        }

        tx, err := db.conn.Begin()
        if err != nil {
                return err
        }
        defer tx.Rollback()

        stmt, err := tx.Prepare(`
                INSERT INTO nav_data (scheme_code, date, nav)
                VALUES (?, ?, ?)
                ON CONFLICT(scheme_code, date) DO UPDATE SET nav=excluded.nav`)
        if err != nil {
                return err
        }
        defer stmt.Close()

        for _, r := range records {
                if _, err := stmt.Exec(r.SchemeCode, r.Date, r.NAV); err != nil {
                        return err
                }
        }
        return tx.Commit()
}

// GetNAVHistoryForRange returns NAV data for a scheme within an inclusive date range.
// fromDate and toDate are in "YYYY-MM-DD" format.
func (db *DB) GetNAVHistoryForRange(schemeCode, fromDate, toDate string) ([]NAVRow, error) {
        rows, err := db.conn.Query(`
                SELECT scheme_code, date, nav
                FROM nav_data
                WHERE scheme_code = ? AND date >= ? AND date <= ?
                ORDER BY date ASC`, schemeCode, fromDate, toDate)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var result []NAVRow
        for rows.Next() {
                var r NAVRow
                if err := rows.Scan(&r.SchemeCode, &r.Date, &r.NAV); err != nil {
                        return nil, err
                }
                result = append(result, r)
        }
        return result, rows.Err()
}

// GetNAVHistory returns all NAV data for a scheme ordered by date asc.
func (db *DB) GetNAVHistory(schemeCode string) ([]NAVRow, error) {
        rows, err := db.conn.Query(`
                SELECT scheme_code, date, nav
                FROM nav_data
                WHERE scheme_code = ?
                ORDER BY date ASC`, schemeCode)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var result []NAVRow
        for rows.Next() {
                var r NAVRow
                if err := rows.Scan(&r.SchemeCode, &r.Date, &r.NAV); err != nil {
                        return nil, err
                }
                result = append(result, r)
        }
        return result, rows.Err()
}

// GetLatestNAV returns the most recent NAV entry for a scheme.
func (db *DB) GetLatestNAV(schemeCode string) (*NAVRow, error) {
        row := db.conn.QueryRow(`
                SELECT scheme_code, date, nav
                FROM nav_data WHERE scheme_code=?
                ORDER BY date DESC LIMIT 1`, schemeCode)
        var r NAVRow
        if err := row.Scan(&r.SchemeCode, &r.Date, &r.NAV); err != nil {
                if err == sql.ErrNoRows {
                        return nil, nil
                }
                return nil, err
        }
        return &r, nil
}

// GetLatestNAVDate returns the latest stored NAV date for a scheme (for incremental sync).
func (db *DB) GetLatestNAVDate(schemeCode string) (string, error) {
        var date string
        err := db.conn.QueryRow(`
                SELECT date FROM nav_data WHERE scheme_code=? ORDER BY date DESC LIMIT 1`,
                schemeCode).Scan(&date)
        if err == sql.ErrNoRows {
                return "", nil
        }
        return date, err
}

// CountNAVRecords returns the number of NAV records for a scheme.
func (db *DB) CountNAVRecords(schemeCode string) (int, error) {
        var n int
        err := db.conn.QueryRow(`SELECT COUNT(*) FROM nav_data WHERE scheme_code=?`, schemeCode).Scan(&n)
        return n, err
}

// ---- Analytics queries -----------------------------------------------------

// UpsertAnalytics saves computed analytics for a fund+window.
func (db *DB) UpsertAnalytics(a AnalyticsRow) error {
        _, err := db.conn.Exec(`
                INSERT INTO analytics (
                        scheme_code, window, rolling_min, rolling_max, rolling_median,
                        rolling_p25, rolling_p75, max_drawdown, cagr_min, cagr_max, cagr_median,
                        rolling_periods_analyzed, start_date, end_date, total_days,
                        nav_data_points, computed_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(scheme_code, window) DO UPDATE SET
                        rolling_min=excluded.rolling_min,
                        rolling_max=excluded.rolling_max,
                        rolling_median=excluded.rolling_median,
                        rolling_p25=excluded.rolling_p25,
                        rolling_p75=excluded.rolling_p75,
                        max_drawdown=excluded.max_drawdown,
                        cagr_min=excluded.cagr_min,
                        cagr_max=excluded.cagr_max,
                        cagr_median=excluded.cagr_median,
                        rolling_periods_analyzed=excluded.rolling_periods_analyzed,
                        start_date=excluded.start_date,
                        end_date=excluded.end_date,
                        total_days=excluded.total_days,
                        nav_data_points=excluded.nav_data_points,
                        computed_at=excluded.computed_at`,
                a.SchemeCode, a.Window, a.RollingMin, a.RollingMax, a.RollingMedian,
                a.RollingP25, a.RollingP75, a.MaxDrawdown, a.CAGRMin, a.CAGRMax, a.CAGRMedian,
                a.RollingPeriodsAnalyzed, a.StartDate, a.EndDate, a.TotalDays,
                a.NAVDataPoints, a.ComputedAt)
        return err
}

// GetAnalytics retrieves computed analytics for a fund+window.
func (db *DB) GetAnalytics(schemeCode, window string) (*AnalyticsRow, error) {
        row := db.conn.QueryRow(`
                SELECT scheme_code, window, rolling_min, rolling_max, rolling_median,
                       rolling_p25, rolling_p75, max_drawdown, cagr_min, cagr_max, cagr_median,
                       rolling_periods_analyzed, start_date, end_date, total_days,
                       nav_data_points, computed_at
                FROM analytics
                WHERE scheme_code=? AND window=?`, schemeCode, window)
        return scanAnalytics(row)
}

// GetAnalyticsByCategory returns analytics rows for all funds in a category and window.
func (db *DB) GetAnalyticsByCategory(category, window string) ([]*AnalyticsRow, error) {
        rows, err := db.conn.Query(`
                SELECT a.scheme_code, a.window, a.rolling_min, a.rolling_max, a.rolling_median,
                       a.rolling_p25, a.rolling_p75, a.max_drawdown, a.cagr_min, a.cagr_max, a.cagr_median,
                       a.rolling_periods_analyzed, a.start_date, a.end_date, a.total_days,
                       a.nav_data_points, a.computed_at
                FROM analytics a
                JOIN schemes s ON s.code = a.scheme_code
                WHERE s.category LIKE ? AND a.window=?`, "%"+category+"%", window)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var result []*AnalyticsRow
        for rows.Next() {
                r, err := scanAnalytics(rows)
                if err != nil {
                        return nil, err
                }
                result = append(result, r)
        }
        return result, rows.Err()
}

// ---- Pipeline state queries ------------------------------------------------

// UpsertPipelineState saves pipeline status for a scheme.
func (db *DB) UpsertPipelineState(code, status string, errMsg *string) error {
        _, err := db.conn.Exec(`
                INSERT INTO pipeline_state (scheme_code, status, updated_at, error)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(scheme_code) DO UPDATE SET
                        status=excluded.status,
                        updated_at=excluded.updated_at,
                        error=excluded.error`,
                code, status, time.Now().UTC().Format(time.RFC3339), errMsg)
        return err
}

// MarkPipelineSynced updates last_synced_at when sync completes.
func (db *DB) MarkPipelineSynced(code string) error {
        now := time.Now().UTC().Format(time.RFC3339)
        _, err := db.conn.Exec(`
                UPDATE pipeline_state SET status='completed', last_synced_at=?, error=NULL, updated_at=?
                WHERE scheme_code=?`, now, now, code)
        return err
}

// GetPipelineStates returns all pipeline states.
func (db *DB) GetPipelineStates() ([]PipelineStateRow, error) {
        rows, err := db.conn.Query(`
                SELECT scheme_code, status, last_synced_at, error, updated_at
                FROM pipeline_state ORDER BY scheme_code`)
        if err != nil {
                return nil, err
        }
        defer rows.Close()

        var result []PipelineStateRow
        for rows.Next() {
                var r PipelineStateRow
                if err := rows.Scan(&r.SchemeCode, &r.Status, &r.LastSyncedAt, &r.Error, &r.UpdatedAt); err != nil {
                        return nil, err
                }
                result = append(result, r)
        }
        return result, rows.Err()
}

// ---- Rate limiter state ----------------------------------------------------

// SaveRateLimiterState persists serialised timestamps for crash recovery.
func (db *DB) SaveRateLimiterState(windowType, requestTimesJSON string) error {
        _, err := db.conn.Exec(`
                INSERT INTO rate_limiter_state (window_type, request_times, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(window_type) DO UPDATE SET
                        request_times=excluded.request_times,
                        updated_at=excluded.updated_at`,
                windowType, requestTimesJSON, time.Now().UTC().Format(time.RFC3339))
        return err
}

// LoadRateLimiterState loads persisted timestamps by window type.
func (db *DB) LoadRateLimiterState(windowType string) (string, error) {
        var v string
        err := db.conn.QueryRow(`
                SELECT request_times FROM rate_limiter_state WHERE window_type=?`, windowType).Scan(&v)
        if err == sql.ErrNoRows {
                return "[]", nil
        }
        return v, err
}

// ---- Sync run log ----------------------------------------------------------

// StartSyncRun records a new sync run and returns its ID.
func (db *DB) StartSyncRun(triggeredBy string) (int64, error) {
        res, err := db.conn.Exec(`
                INSERT INTO sync_runs (started_at, triggered_by, status)
                VALUES (?, ?, 'running')`, time.Now().UTC().Format(time.RFC3339), triggeredBy)
        if err != nil {
                return 0, err
        }
        return res.LastInsertId()
}

// EndSyncRun marks a sync run as complete.
func (db *DB) EndSyncRun(id int64, status string, errMsg *string) error {
        _, err := db.conn.Exec(`
                UPDATE sync_runs SET ended_at=?, status=?, error=? WHERE id=?`,
                time.Now().UTC().Format(time.RFC3339), status, errMsg, id)
        return err
}

// ---- Row types (internal, not exported as models) --------------------------

type SchemeRow struct {
        Code          string
        Name          string
        AMC           string
        Category      string
        DiscoveredAt  string
        LastSyncedAt  *string
}

type NAVRow struct {
        SchemeCode string
        Date       string
        NAV        float64
}

type AnalyticsRow struct {
        SchemeCode             string
        Window                 string
        RollingMin             float64
        RollingMax             float64
        RollingMedian          float64
        RollingP25             float64
        RollingP75             float64
        MaxDrawdown            float64
        CAGRMin                float64
        CAGRMax                float64
        CAGRMedian             float64
        RollingPeriodsAnalyzed int
        StartDate              string
        EndDate                string
        TotalDays              int
        NAVDataPoints          int
        ComputedAt             string
}

type PipelineStateRow struct {
        SchemeCode   string
        Status       string
        LastSyncedAt *string
        Error        *string
        UpdatedAt    string
}

// ---- Helpers ---------------------------------------------------------------

type rowScanner interface {
        Scan(dest ...interface{}) error
}

func scanScheme(row rowScanner) (*SchemeRow, error) {
        var s SchemeRow
        if err := row.Scan(&s.Code, &s.Name, &s.AMC, &s.Category, &s.DiscoveredAt, &s.LastSyncedAt); err != nil {
                if err == sql.ErrNoRows {
                        return nil, nil
                }
                return nil, err
        }
        return &s, nil
}

func scanAnalytics(row rowScanner) (*AnalyticsRow, error) {
        var a AnalyticsRow
        err := row.Scan(
                &a.SchemeCode, &a.Window,
                &a.RollingMin, &a.RollingMax, &a.RollingMedian,
                &a.RollingP25, &a.RollingP75,
                &a.MaxDrawdown,
                &a.CAGRMin, &a.CAGRMax, &a.CAGRMedian,
                &a.RollingPeriodsAnalyzed,
                &a.StartDate, &a.EndDate, &a.TotalDays, &a.NAVDataPoints,
                &a.ComputedAt,
        )
        if err != nil {
                if err == sql.ErrNoRows {
                        return nil, nil
                }
                return nil, err
        }
        return &a, nil
}
