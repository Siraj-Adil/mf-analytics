# Mutual Fund Analytics — Design Decisions

## 1. Rate-Limiting Strategy

### Algorithm: Sliding Window Log (Three-Tier)

The service enforces three simultaneous limits mandated by mfapi.in:
- **2 requests/second**
- **50 requests/minute**  
- **300 requests/hour**

We use a **sliding window log** per tier: a sorted list of timestamps of recent requests. For each window, we discard timestamps older than the window duration before checking the count.

#### Why not a token bucket?

Token buckets are stateless and efficient, but they have "burst" behaviour at window boundaries — a token bucket at 2 req/sec allows 4 requests if timed correctly across a boundary. The mfapi.in limits appear to be enforced as sliding windows, so we match that model exactly.

#### Why not a fixed window counter?

Fixed windows suffer from the "double-window" problem: up to 2× the limit can be sent in bursts at window edges. Sliding window logs eliminate this.

#### Correctness Proof

1. **Atomic check-and-record**: The mutex is held for the entire check-record sequence. It is impossible for two concurrent callers to both observe "under limit" and both record — one will observe the other's record.

2. **All-or-nothing across tiers**: We check all three limits before recording any. If any one tier is at capacity, no tier records the request. This prevents partial consumption (token "leaks").

3. **Accurate count**: Stale entries are removed before counting. The count always reflects the true number of requests within [now − window, now].

4. **Persistence across restarts**: Request timestamps are serialized to SQLite after every `Allow()` call (async, non-blocking). On startup, the limiter loads these timestamps and discards any older than the respective window, so the rate-limit history is correctly restored after a crash or restart.

### WaitAndAcquire

When a request cannot be served immediately, `WaitAndAcquire` polls in a loop. Each wait interval is computed as the time until the oldest request in the most-constrained window expires — avoiding unnecessary CPU spin while minimising latency.

---

## 2. Backfill Orchestration Within Quota Constraints

### The Numbers

With 300 requests/hour and 10 schemes, each scheme can receive at most 30 API calls per hour. A single `GET /mf/{code}` returns the **full historical NAV** (potentially 10 years of data), so **one request per scheme suffices for backfill**. This leaves us comfortably within the hourly quota.

### Discovery + Backfill Sequence

```
1. GET /mf/         — one request to fetch the full catalogue (~37,000 schemes)
2. Filter by name   — no API calls; filter by AMC + category + "Direct" + "Growth"
3. GET /mf/{code}   — one rate-limited request per candidate to confirm metadata
4. Store NAV data   — bulk SQLite upsert (no API call)
5. Compute analytics— pure CPU, no API call
```

Total API calls for 10 schemes: **1 (catalogue) + 10 (details) + 10 (verification) ≤ 25 requests**.  
This is well within the 300/hour limit and completes in under 1 minute at 2 req/sec.

### Incremental Sync (Daily)

Daily updates run `GET /mf/{code}` per scheme (10 calls/day), then filter to dates newer than `last_synced_at`. The rate limiter automatically handles the 2 req/sec pacing.

### Resumability After Crash

Every scheme has a row in `pipeline_state` with status `pending | in_progress | completed | failed`.

- On restart, the pipeline reads this table.
- `completed` schemes skip backfill but still run incremental sync.
- `pending` and `failed` schemes are re-processed from the beginning.
- `in_progress` (crash mid-flight) are retried; the NAV upsert is idempotent so partial writes are safe.

---

## 3. Storage Schema for Time-Series NAV Data

We use **SQLite with WAL mode** for the following reasons:
- Zero external infrastructure (no Postgres, no Redis)
- WAL enables concurrent reads during writes
- Time-series queries (order by date, range scans) are efficiently served by the B-tree index on `(scheme_code, date)`
- Single-file persistence simplifies deployment and backup

### Key Tables

| Table | Purpose |
|---|---|
| `schemes` | Fund metadata (code, name, AMC, category) |
| `nav_data` | Daily NAV time series; PRIMARY KEY (scheme_code, date) |
| `analytics` | Pre-computed results per (scheme_code, window) |
| `pipeline_state` | Per-scheme sync lifecycle; enables resumability |
| `rate_limiter_state` | Serialised request timestamps; survives restarts |
| `sync_runs` | Audit log of every pipeline run |

The `nav_data` table has a compound primary key on `(scheme_code, date)` which acts as the covering index for the most common query pattern: "give me all NAV for scheme X ordered by date". Because SQLite stores rows in primary-key order, this query is a sequential scan with no extra sort step.

### Why Not a Dedicated Time-Series DB?

InfluxDB or TimescaleDB would offer better compression and time-bucketing functions, but introduce significant operational overhead. With only 10 schemes × ~250 trading days per year × 10 years ≈ 25,000 rows, SQLite is more than sufficient.

---

## 4. Pre-Computation vs On-Demand Trade-Offs

### Decision: Pre-Compute, Always

Analytics are computed once after each sync and stored in the `analytics` table. API reads are simple `SELECT` statements — no computation at query time.

**Rationale:**

| Dimension | Pre-compute | On-demand |
|---|---|---|
| API latency | <5 ms (single row read) | ~200–500 ms (full NAV scan + sort + stats) |
| Data freshness | Updated after each sync | Always current |
| CPU spike | Background, post-sync | Per-request; affects p99 under load |
| Complexity | Slightly more storage | Simpler pipeline |

Given the assignment's explicit `<200 ms` API latency requirement and the fact that analytics only need to be as fresh as the last NAV update (once daily), pre-computation is clearly the right choice.

### What Gets Pre-Computed

For each `(scheme_code, window)` pair across `{1Y, 3Y, 5Y, 10Y}`:
- Rolling returns: min, max, median, p25, p75 (over all rolling periods)
- Max drawdown (worst peak-to-trough decline in percentage)
- CAGR distribution: min, max, median (annualised rolling return)

**Rolling period definition**: For window `N` years, for every trading day `t` in the dataset where `t − N*365 days` has a NAV data point (within ±30 calendar days tolerance for weekends/holidays), we compute the annualised return.

---

## 5. Handling Schemes with Insufficient History

Some schemes may have fewer than `N` years of NAV history. We handle this gracefully:

1. **Silent skip**: If fewer than `N` years of calendar data exist, `computeWindow` returns an error that is logged but does not fail the overall analytics run. No row is written to the `analytics` table for that window.

2. **Minimum periods check**: Even if the date range is long enough, we require at least **12 rolling periods** to compute meaningful statistics. If fewer exist (e.g., due to data gaps), the window is skipped.

3. **API response**: If analytics are not available for a requested window, the API returns HTTP 404 with a descriptive message: *"analytics not yet computed; trigger a sync first"* — which is accurate whether the data is missing or the window is genuinely unsupported.

4. **Future evolution**: Once a fund accumulates sufficient history, the next analytics re-computation will automatically fill in the previously-skipped windows. No manual intervention is required.

---

## 6. What can be improved?

The system is designed to answer arbitrary time-range queries, not just fixed windows:

- **Caching on Range Endpoint**: Add caching for GET /funds/{code}/analytics/range (keyed by code+from+to+rolling_window) to avoid recomputing repeated queries.

- **Adding Custom windows**: The analytics engine's `computeWindow` function accepts any `AnalyticsWindow`; adding a new window (e.g., `6M`) requires only a config change and an analytics re-computation.

- **Cross-fund comparisons**: The ranking endpoint (`GET /funds/rank`) already supports sorting by median return or max drawdown. It can be extended to support additional metrics (Sharpe ratio, Sortino ratio) by adding computed columns to the `analytics` table.

**Security and ops**: Protect REST endpoints with authentication/authorization and add client-level rate limiting to prevent abuse, scraping, and denial-of-service patterns.

---

## 7. Reasoning Summary

| Area | Decision | Reason |
|---|---|---|
| Rate limiter algorithm | Sliding window log | Exact match to mfapi.in's enforced model; no bursts |
| Backfill strategy | One API call per scheme (full history returned) | mfapi.in returns all history in a single response |
| Storage choice | SQLite + WAL | Zero infrastructure overhead; sufficient for 10-scheme dataset |
| Caching strategy | Pre-computed analytics table | Meets <200 ms latency; freshness is daily at most |
| Failure handling | Per-scheme state machine; idempotent upserts | Crash-safe; any scheme can be retried without data loss |
