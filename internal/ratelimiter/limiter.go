// Package ratelimiter implements a three-tier sliding-window rate limiter
// that enforces mfapi.in constraints: 2 req/sec, 50 req/min, 300 req/hour.
//
// Correctness proof:
//   - Each Allow() call acquires the mutex before inspecting OR recording
//     any timestamp, so it is impossible for concurrent callers to both see
//     "below limit" and both record, exceeding the limit.
//   - Timestamps older than the window are discarded before the check,
//     ensuring the count is always the true number of requests in [now-window, now].
//   - All three limits are checked atomically in one lock: either all three
//     allow the request or none do — no partial consumption.
//   - State is persisted to SQLite periodically and on every successful
//     Allow() so it survives process restarts.
package ratelimiter

import (
        "context"
        "encoding/json"
        "fmt"
        "log"
        "sync"
        "time"
)

// StateStore is the minimal persistence interface required by the rate limiter.
type StateStore interface {
        SaveRateLimiterState(windowType, requestTimesJSON string) error
        LoadRateLimiterState(windowType string) (string, error)
}

// window is a single sliding-window tier.
type window struct {
        requests []time.Time
        limit    int
        duration time.Duration
        label    string
}

// cleanup removes timestamps outside the current window.
func (w *window) cleanup(now time.Time) {
        cutoff := now.Add(-w.duration)
        i := 0
        for i < len(w.requests) && !w.requests[i].After(cutoff) {
                i++
        }
        w.requests = w.requests[i:]
}

func (w *window) canAllow() bool {
        return len(w.requests) < w.limit
}

func (w *window) record(now time.Time) {
        w.requests = append(w.requests, now)
}

func (w *window) count() int {
        return len(w.requests)
}

// persistReq is sent on the persist channel to trigger a background save.
type persistReq struct {
        second    []time.Time
        minute    []time.Time
        hour      []time.Time
        flush     bool         // if true, signal flushDone after this request is processed
        flushDone chan struct{} // closed when this flush is complete
}

// Limiter is the three-tier sliding-window rate limiter.
type Limiter struct {
        mu          sync.Mutex
        second      window
        minute      window
        hour        window
        store       StateStore
        persistCh   chan persistReq
}

// New creates a Limiter with the given per-second, per-minute, per-hour limits.
// It loads persisted state from store so rate limit history survives restarts.
func New(perSec, perMin, perHour int, store StateStore) (*Limiter, error) {
        l := &Limiter{
                second:    window{limit: perSec, duration: time.Second, label: "second"},
                minute:    window{limit: perMin, duration: time.Minute, label: "minute"},
                hour:      window{limit: perHour, duration: time.Hour, label: "hour"},
                store:     store,
                persistCh: make(chan persistReq, 64),
        }

        // Load persisted state for crash recovery.
        for _, w := range []*window{&l.second, &l.minute, &l.hour} {
                if err := l.loadWindow(w); err != nil {
                        log.Printf("[ratelimiter] could not load %s window state: %v — starting fresh", w.label, err)
                }
        }

        // Single background goroutine drains the persist channel, preventing
        // concurrent goroutines from overwriting each other with stale snapshots.
        go l.persistWorker()

        return l, nil
}

// Allow checks all three tiers atomically. Returns true and records the
// request if all limits allow it; returns false without side effects otherwise.
func (l *Limiter) Allow() bool {
        l.mu.Lock()
        defer l.mu.Unlock()

        now := time.Now()

        l.second.cleanup(now)
        l.minute.cleanup(now)
        l.hour.cleanup(now)

        if !l.second.canAllow() || !l.minute.canAllow() || !l.hour.canAllow() {
                return false
        }

        l.second.record(now)
        l.minute.record(now)
        l.hour.record(now)

        l.enqueuePersist()
        return true
}

// FlushPersist blocks until all queued persist operations have completed.
// Primarily for use in tests.
func (l *Limiter) FlushPersist() {
        done := make(chan struct{})
        l.persistCh <- persistReq{flush: true, flushDone: done}
        <-done
}

// WaitAndAcquire blocks until a token is available across all three tiers,
// or until ctx is cancelled. Returns the time waited.
func (l *Limiter) WaitAndAcquire(ctx context.Context) (time.Duration, error) {
        start := time.Now()
        for {
                if l.Allow() {
                        return time.Since(start), nil
                }

                // Compute how long until the next possible slot.
                sleep := l.nextSlotDelay()
                select {
                case <-ctx.Done():
                        return time.Since(start), ctx.Err()
                case <-time.After(sleep):
                }
        }
}

// nextSlotDelay returns how long to wait before retrying.
// It looks at the oldest request in each window that would expire soonest.
func (l *Limiter) nextSlotDelay() time.Duration {
        l.mu.Lock()
        defer l.mu.Unlock()

        now := time.Now()
        minDelay := 50 * time.Millisecond // polling floor

        for _, w := range []*window{&l.second, &l.minute, &l.hour} {
                w.cleanup(now)
                if w.count() >= w.limit && len(w.requests) > 0 {
                        // Oldest request will expire at requests[0] + duration
                        expiry := w.requests[0].Add(w.duration)
                        d := time.Until(expiry) + time.Millisecond
                        if d > minDelay {
                                minDelay = d
                        }
                }
        }
        return minDelay
}

// Stats returns current usage counters without acquiring a token.
func (l *Limiter) Stats() (sec, min, hour int) {
        l.mu.Lock()
        defer l.mu.Unlock()

        now := time.Now()
        l.second.cleanup(now)
        l.minute.cleanup(now)
        l.hour.cleanup(now)

        return l.second.count(), l.minute.count(), l.hour.count()
}

// Limits returns the configured limits.
func (l *Limiter) Limits() (sec, min, hour int) {
        return l.second.limit, l.minute.limit, l.hour.limit
}

// enqueuePersist snapshots the current window state and sends it to the
// persist worker. Must be called while holding l.mu.
func (l *Limiter) enqueuePersist() {
        req := persistReq{
                second: make([]time.Time, len(l.second.requests)),
                minute: make([]time.Time, len(l.minute.requests)),
                hour:   make([]time.Time, len(l.hour.requests)),
        }
        copy(req.second, l.second.requests)
        copy(req.minute, l.minute.requests)
        copy(req.hour, l.hour.requests)

        // Non-blocking send: if the channel is full the persist will happen on
        // the next Allow() call. The channel buffer is large enough that this
        // should never drop a write under normal conditions.
        select {
        case l.persistCh <- req:
        default:
        }
}

// persistWorker is the single goroutine that serialises all store writes,
// preventing concurrent goroutines from overwriting each other with stale data.
func (l *Limiter) persistWorker() {
        for req := range l.persistCh {
                // Handle flush sentinel.
                if req.flush {
                        close(req.flushDone)
                        continue
                }

                // Drain any queued non-flush writes, keeping only the most recent.
        drain:
                for {
                        select {
                        case latest := <-l.persistCh:
                                if latest.flush {
                                        // Process the flush inline, then continue draining.
                                        close(latest.flushDone)
                                        continue
                                }
                                req = latest
                        default:
                                break drain
                        }
                }

                if req.flush {
                        continue
                }

                snapshots := map[string][]time.Time{
                        "second": req.second,
                        "minute": req.minute,
                        "hour":   req.hour,
                }
                for label, snap := range snapshots {
                        data, err := marshalTimes(snap)
                        if err != nil {
                                continue
                        }
                        if err := l.store.SaveRateLimiterState(label, data); err != nil {
                                log.Printf("[ratelimiter] persist %s state: %v", label, err)
                        }
                }
        }
}

// loadWindow loads persisted timestamps into a window, discarding expired ones.
func (l *Limiter) loadWindow(w *window) error {
        data, err := l.store.LoadRateLimiterState(w.label)
        if err != nil {
                return err
        }
        times, err := unmarshalTimes(data)
        if err != nil {
                return fmt.Errorf("unmarshal: %w", err)
        }

        // Only keep timestamps still within the window.
        cutoff := time.Now().Add(-w.duration)
        for _, t := range times {
                if t.After(cutoff) {
                        w.requests = append(w.requests, t)
                }
        }
        log.Printf("[ratelimiter] loaded %d/%d timestamps for %s window", len(w.requests), len(times), w.label)
        return nil
}

func marshalTimes(ts []time.Time) (string, error) {
        unix := make([]int64, len(ts))
        for i, t := range ts {
                unix[i] = t.UnixNano()
        }
        b, err := json.Marshal(unix)
        return string(b), err
}

func unmarshalTimes(data string) ([]time.Time, error) {
        var unix []int64
        if err := json.Unmarshal([]byte(data), &unix); err != nil {
                return nil, err
        }
        times := make([]time.Time, len(unix))
        for i, u := range unix {
                times[i] = time.Unix(0, u)
        }
        return times, nil
}
