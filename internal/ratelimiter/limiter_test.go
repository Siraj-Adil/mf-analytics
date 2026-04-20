package ratelimiter_test

import (
        "context"
        "sync"
        "testing"
        "time"

        "github.com/mutual-fund-analytics/internal/ratelimiter"
)

// mockStore is an in-memory StateStore for tests.
type mockStore struct {
        mu   sync.Mutex
        data map[string]string
}

func newMockStore() *mockStore {
        return &mockStore{data: map[string]string{
                "second": "[]",
                "minute": "[]",
                "hour":   "[]",
        }}
}

func (m *mockStore) SaveRateLimiterState(key, value string) error {
        m.mu.Lock()
        defer m.mu.Unlock()
        m.data[key] = value
        return nil
}

func (m *mockStore) LoadRateLimiterState(key string) (string, error) {
        m.mu.Lock()
        defer m.mu.Unlock()
        if v, ok := m.data[key]; ok {
                return v, nil
        }
        return "[]", nil
}

// TestPerSecondLimit verifies at most 2 requests are allowed per second.
func TestPerSecondLimit(t *testing.T) {
        l, _ := ratelimiter.New(2, 100, 1000, newMockStore())

        allowed := 0
        for i := 0; i < 10; i++ {
                if l.Allow() {
                        allowed++
                }
        }

        if allowed != 2 {
                t.Errorf("expected 2 allowed in burst, got %d", allowed)
        }
}

// TestPerMinuteLimit verifies the per-minute limit is enforced independently.
func TestPerMinuteLimit(t *testing.T) {
        // 10/sec allows many per second, but only 3 per minute.
        l, _ := ratelimiter.New(10, 3, 1000, newMockStore())

        allowed := 0
        for i := 0; i < 20; i++ {
                if l.Allow() {
                        allowed++
                }
        }

        if allowed != 3 {
                t.Errorf("expected 3 allowed by minute limit, got %d", allowed)
        }
}

// TestPerHourLimit verifies the per-hour limit is enforced independently.
func TestPerHourLimit(t *testing.T) {
        l, _ := ratelimiter.New(100, 100, 5, newMockStore())

        allowed := 0
        for i := 0; i < 20; i++ {
                if l.Allow() {
                        allowed++
                }
        }

        if allowed != 5 {
                t.Errorf("expected 5 allowed by hour limit, got %d", allowed)
        }
}

// TestAllThreeLimitsSimultaneous verifies all three limits apply at once.
func TestAllThreeLimitsSimultaneous(t *testing.T) {
        // per-sec=3, per-min=2, per-hour=10 → only 2 should pass (per-minute is tightest)
        l, _ := ratelimiter.New(3, 2, 10, newMockStore())

        allowed := 0
        for i := 0; i < 20; i++ {
                if l.Allow() {
                        allowed++
                }
        }

        if allowed != 2 {
                t.Errorf("expected 2 (per-minute limit), got %d", allowed)
        }
}

// TestAtomicAllOrNothing ensures partial consumption never occurs.
func TestAtomicAllOrNothing(t *testing.T) {
        // per-sec=1, per-min=1, per-hour=5
        // First call should succeed. Second should fail on ALL three (none consumed).
        l, _ := ratelimiter.New(1, 1, 5, newMockStore())

        first := l.Allow()
        second := l.Allow()

        if !first {
                t.Fatal("expected first call to be allowed")
        }
        if second {
                t.Fatal("expected second call to be denied by per-second AND per-minute limits")
        }

        // Stats should show exactly 1 in second and minute windows.
        sec, min, _ := l.Stats()
        if sec != 1 {
                t.Errorf("second window count = %d, want 1", sec)
        }
        if min != 1 {
                t.Errorf("minute window count = %d, want 1", min)
        }
}

// TestConcurrentAccess verifies thread-safety under concurrent load.
func TestConcurrentAccess(t *testing.T) {
        const limit = 10
        l, _ := ratelimiter.New(limit, 1000, 10000, newMockStore())

        var wg sync.WaitGroup
        var mu sync.Mutex
        allowed := 0

        for i := 0; i < 50; i++ {
                wg.Add(1)
                go func() {
                        defer wg.Done()
                        if l.Allow() {
                                mu.Lock()
                                allowed++
                                mu.Unlock()
                        }
                }()
        }

        wg.Wait()

        if allowed > limit {
                t.Errorf("concurrent access exceeded limit: got %d, want <= %d", allowed, limit)
        }
}

// TestWaitAndAcquireContextCancel verifies WaitAndAcquire respects context cancellation.
func TestWaitAndAcquireContextCancel(t *testing.T) {
        // Saturate all limits so WaitAndAcquire has to wait.
        l, _ := ratelimiter.New(1, 1, 1, newMockStore())
        l.Allow() // consume the one available token

        ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
        defer cancel()

        _, err := l.WaitAndAcquire(ctx)
        if err == nil {
                t.Error("expected context cancellation error, got nil")
        }
}

// TestStatePersistenceAndRecovery verifies that persisted state is correctly
// restored, so a restarted process does not exceed limits.
func TestStatePersistenceAndRecovery(t *testing.T) {
        store := newMockStore()

        // Create a limiter, exhaust the per-second limit.
        l1, _ := ratelimiter.New(2, 100, 1000, store)
        l1.Allow()
        l1.Allow()

        // FlushPersist ensures the persist worker has written both timestamps
        // before we create l2. This is deterministic and race-free.
        l1.FlushPersist()

        // Create a new limiter loading the same store — should still be at limit.
        l2, _ := ratelimiter.New(2, 100, 1000, store)
        if l2.Allow() {
                t.Error("expected restored limiter to deny request (still within second window)")
        }
}

// TestStatsReporting verifies Stats() returns accurate counts.
func TestStatsReporting(t *testing.T) {
        l, _ := ratelimiter.New(5, 50, 300, newMockStore())

        l.Allow()
        l.Allow()
        l.Allow()

        sec, _, _ := l.Stats()
        if sec != 3 {
                t.Errorf("expected 3 requests in second window, got %d", sec)
        }
}

// TestSlidingWindowExpiry verifies old requests fall out of the window.
func TestSlidingWindowExpiry(t *testing.T) {
        if testing.Short() {
                t.Skip("skipping timing-sensitive test in short mode")
        }

        // 2/sec window. Allow 2, wait 1.1s, should allow 2 more.
        l, _ := ratelimiter.New(2, 100, 1000, newMockStore())

        if !l.Allow() || !l.Allow() {
                t.Fatal("first two requests should be allowed")
        }
        if l.Allow() {
                t.Error("third request should be denied within same second")
        }

        time.Sleep(1100 * time.Millisecond)

        if !l.Allow() {
                t.Error("request should be allowed after window expiry")
        }
}
