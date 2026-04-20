package mfapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RateLimiter is the interface the client uses for throttling.
type RateLimiter interface {
	WaitAndAcquire(ctx context.Context) (time.Duration, error)
}

// Client is a rate-limited HTTP client for mfapi.in.
type Client struct {
	base    string
	http    *http.Client
	limiter RateLimiter
}

// NewClient returns a Client. base is e.g. "https://api.mfapi.in".
func NewClient(base string, limiter RateLimiter) *Client {
	return &Client{
		base:    strings.TrimRight(base, "/"),
		limiter: limiter,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// FetchSchemeList fetches all scheme codes and names in one request.
// The full-catalogue endpoint is not subject to the same per-call rate limits,
// but we still count it against the limiter for safety.
func (c *Client) FetchSchemeList(ctx context.Context) ([]SchemeListItem, error) {
	waited, err := c.limiter.WaitAndAcquire(ctx)
	if err != nil {
		return nil, err
	}
	if waited > 0 {
		log.Printf("[mfapi] waited %v for rate-limit slot (scheme list)", waited)
	}

	url := c.base + "/mf/"
	resp, err := c.doGet(ctx, url)
	if err != nil {
		return nil, err
	}

	var items []SchemeListItem
	if err := json.Unmarshal(resp, &items); err != nil {
		return nil, fmt.Errorf("decode scheme list: %w", err)
	}
	log.Printf("[mfapi] fetched %d schemes from catalogue", len(items))
	return items, nil
}

// FetchSchemeDetail fetches full metadata + NAV history for a single scheme.
func (c *Client) FetchSchemeDetail(ctx context.Context, code string) (*SchemeDetail, error) {
	waited, err := c.limiter.WaitAndAcquire(ctx)
	if err != nil {
		return nil, err
	}
	if waited > 50*time.Millisecond {
		log.Printf("[mfapi] waited %v for rate-limit slot (scheme %s)", waited.Round(time.Millisecond), code)
	}

	url := fmt.Sprintf("%s/mf/%s", c.base, code)
	body, err := c.doGet(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetch scheme %s: %w", code, err)
	}

	var detail SchemeDetail
	if err := json.Unmarshal(body, &detail); err != nil {
		return nil, fmt.Errorf("decode scheme %s: %w", code, err)
	}
	if detail.Status != "SUCCESS" {
		return nil, fmt.Errorf("API returned non-success for scheme %s: %s", code, detail.Status)
	}
	return &detail, nil
}

// ParseNAVDate parses mfapi.in's DD-MM-YYYY date format.
func ParseNAVDate(raw string) (time.Time, error) {
	return time.Parse("02-01-2006", raw)
}

// ParseNAV parses the NAV string to float64.
func ParseNAV(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid NAV %q: %w", raw, err)
	}
	if v <= 0 {
		return 0, fmt.Errorf("non-positive NAV %q", raw)
	}
	return v, nil
}

// doGet performs a GET with retry on 429 and transient errors.
func (c *Client) doGet(ctx context.Context, url string) ([]byte, error) {
	const maxRetries = 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(attempt*attempt) * 2 * time.Second
			log.Printf("[mfapi] retry %d for %s after %v", attempt, url, backoff)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "MutualFundAnalytics/1.0")

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("http do: %w", err)
			continue
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, 20*1024*1024))
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("read body: %w", err)
			continue
		}

		switch resp.StatusCode {
		case http.StatusOK:
			return body, nil
		case http.StatusTooManyRequests:
			// mfapi.in rate-limit hit — back off 5 seconds minimum.
			log.Printf("[mfapi] 429 Too Many Requests for %s — backing off 5s", url)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Second):
			}
			lastErr = fmt.Errorf("429 rate limited")
		default:
			lastErr = fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
		}
	}

	return nil, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}
