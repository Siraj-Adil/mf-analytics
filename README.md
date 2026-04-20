# Mutual Fund Analytics API

A Go-based analytics service that ingests daily mutual fund NAV data from [mfapi.in](https://api.mfapi.in), stores it in SQLite, computes return/risk metrics, and exposes HTTP APIs for fund discovery, analytics, ranking, and sync status.

## What This Project Does

- Discovers schemes for target AMCs and categories.
- Pulls full historical daily NAV data for each tracked fund.
- Performs incremental sync for new NAV entries on subsequent runs.
- Pre-computes analytics for `1Y`, `3Y`, `5Y`, `10Y` windows.
- Serves API endpoints for:
  - fund list/detail
  - precomputed analytics
  - date-range analytics
  - fund ranking
  - sync trigger/status

## Tech Stack

- **Language:** Go `1.21`
- **HTTP Router:** `go-chi/chi`
- **Database:** SQLite (`modernc.org/sqlite`)
- **External API:** `https://api.mfapi.in`
- **Build/Test:** `Makefile`, `go test`

## Project Structure

```text
cmd/server/main.go            # App entry point and dependency wiring
internal/config/              # Env-based configuration
internal/db/                  # SQLite schema, migrations, query layer
internal/mfapi/               # Typed mfapi.in client + parsing helpers
internal/ratelimiter/         # Sliding-window limiter (sec/min/hour)
internal/pipeline/            # Discovery + NAV sync + analytics orchestration
internal/analytics/           # Core analytics computations
internal/api/                 # HTTP handlers and routes
internal/models/              # Shared response/domain models
DESIGN_DECISIONS.md           # Design rationale and trade-off notes
```

## Execution Flow (High Level)

1. `main()` loads config.
2. Opens SQLite DB and runs migrations.
3. Initializes rate limiter (restores persisted limiter state).
4. Creates mfapi client, analytics engine, and pipeline.
5. Builds HTTP handler/router and starts server.
6. Launches:
   - startup auto-sync
   - daily scheduled sync (every 24h)
7. Handles API requests concurrently.

## Entry Point and Wiring

Main entry point: `cmd/server/main.go`

Core dependency graph:

```text
main
 ├─ config.Load
 ├─ db.New
 ├─ ratelimiter.New
 ├─ mfapi.NewClient
 ├─ analytics.New
 ├─ pipeline.New
 ├─ api.NewHandler
 └─ api.NewRouter -> http.Server
```

## Rate Limiting

All outbound mfapi calls go through a 3-tier sliding-window limiter:

- `2 requests / second`
- `50 requests / minute`
- `300 requests / hour`

Limiter state is persisted to SQLite (`rate_limiter_state`) so request history survives process restarts.

## Pipeline Behavior

Pipeline (`internal/pipeline`) runs in three stages:

1. **Discover schemes**
   - Fetch full catalogue (`/mf/`)
   - Filter by target AMC/category and fund naming constraints
   - Fetch details for candidates (`/mf/{code}`), then store confirmed schemes
2. **Sync NAV data**
   - New/failed funds: full backfill from detail API data
   - Completed funds: incremental sync using last NAV date
3. **Compute analytics**
   - Recompute analytics windows for all eligible schemes

Pipeline is resumable via `pipeline_state` table (`pending`, `in_progress`, `completed`, `failed`).

## Analytics Engine

Analytics are computed in `internal/analytics/engine.go`.

For each scheme and window (`1Y`, `3Y`, `5Y`, `10Y`):

- Rolling return % distribution:
  - `min`, `max`, `median`, `p25`, `p75`
- CAGR % distribution:
  - `min`, `max`, `median`
- Max drawdown %

Core formulas:

- **Rolling total return (%)**
  - `((endNAV / startNAV) - 1) * 100`
- **Rolling CAGR (%)**
  - `((endNAV / startNAV)^(1/years) - 1) * 100`
- **Drawdown at point (%)**
  - `((nav - peak) / peak) * 100`
- **Max drawdown**
  - Most negative drawdown observed in series

Range analytics endpoint computes on-the-fly metrics for arbitrary `from/to` dates and optional rolling window.

## Database Schema

Managed in `internal/db/db.go`:

- `schemes` - fund metadata
- `nav_data` - daily NAV series (`scheme_code`, `date`) PK
- `analytics` - precomputed metrics by (`scheme_code`, `window`)
- `pipeline_state` - sync progress per scheme
- `rate_limiter_state` - persisted limiter timestamps
- `sync_runs` - run-level audit records

## API Endpoints

### Health

- `GET /healthz`

### Funds

- `GET /funds?amc=&category=`
- `GET /funds/{code}`

### Analytics

- `GET /funds/{code}/analytics?window=1Y|3Y|5Y|10Y`
- `GET /funds/{code}/analytics/range?from=YYYY-MM-DD&to=YYYY-MM-DD[&rolling_window=1Y|3Y|5Y|10Y]`

### Ranking

- `GET /funds/rank?category=...&window=...&sort_by=median_return|max_drawdown&limit=5`

### Sync

- `POST /sync/trigger`
- `GET /sync/status`

## Configuration

Environment variables (`internal/config/config.go`):

| Variable                | Default                | Purpose                             |
| ----------------------- | ---------------------- | ----------------------------------- |
| `PORT`                  | `9000`                 | HTTP server port                    |
| `DB_PATH`               | `mf_analytics.db`      | SQLite file path                    |
| `LOG_LEVEL`             | `info`                 | Log level (currently informational) |
| `MFAPI_BASE`            | `https://api.mfapi.in` | Base URL for external API           |
| `RATE_LIMIT_PER_SECOND` | `2`                    | Limiter threshold                   |
| `RATE_LIMIT_PER_MINUTE` | `50`                   | Limiter threshold                   |
| `RATE_LIMIT_PER_HOUR`   | `300`                  | Limiter threshold                   |
| `BACKFILL_WORKERS`      | `1`                    | Backfill worker setting             |

## Getting Started

### Prerequisites

- Go `1.21+`

### Install dependencies

```bash
go mod tidy
```

### Run server

```bash
go run ./cmd/server/
```

Or via Makefile:

```bash
make run
```

Server starts on `http://localhost:9000` by default.

## Build and Test

Build:

```bash
make build
```

Run all tests:

```bash
make test
```

Focused test suites:

```bash
make test-rate-limiter
make test-analytics
make test-pipeline
```

## Example Requests

Health:

```bash
curl http://localhost:9000/healthz
```

List funds:

```bash
curl "http://localhost:9000/funds?category=Mid%20Cap"
```

Fund analytics:

```bash
curl "http://localhost:9000/funds/120503/analytics?window=3Y"
```

Range analytics:

```bash
curl "http://localhost:9000/funds/120503/analytics/range?from=2020-01-01&to=2023-12-31&rolling_window=1Y"
```

Trigger sync:

```bash
curl -X POST http://localhost:9000/sync/trigger
```

Sync status:

```bash
curl http://localhost:9000/sync/status
```

## Notes

- Initial startup may take time while first sync/discovery runs.
- Analytics for a window may be unavailable if history is insufficient.
- If analytics are missing, trigger sync and check `/sync/status`.

## Design Reference

For architectural decisions and trade-offs, see [DESIGN_DECISIONS.md](./DESIGN_DECISIONS.md).
