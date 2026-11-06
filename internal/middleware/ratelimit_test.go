package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSlidingWindowCounter(t *testing.T) {
	limit := int64(3)
	window := 1 * time.Second
	sw := NewSlidingWindowCounter(limit, window)

	now := time.Now()
	// First 3 requests should be allowed
	for i := 0; i < 3; i++ {
		res := sw.Allow(now)
		if !res.Allowed {
			t.Fatalf("Expected request %d to be allowed", i+1)
		}
	}

	// 4th request should be blocked
	res := sw.Allow(now)
	if res.Allowed {
		t.Fatalf("Expected 4th request to be blocked")
	}

	// Move time past window
	future := now.Add(1100 * time.Millisecond)
	res = sw.Allow(future)
	if !res.Allowed {
		t.Fatalf("Expected request after window expiry to be allowed")
	}
}

func TestLeakyBucket(t *testing.T) {
	capacity := int64(2)
	leakInterval := 200 * time.Millisecond
	lb := NewLeakyBucket(capacity, leakInterval)

	now := time.Now()
	// Fill capacity (2 requests)
	if res := lb.Allow(now); !res.Allowed {
		t.Fatalf("Expected 1st request allowed")
	}
	if res := lb.Allow(now); !res.Allowed {
		t.Fatalf("Expected 2nd request allowed")
	}
	// 3rd request overflows bucket
	if res := lb.Allow(now); res.Allowed {
		t.Fatalf("Expected 3rd request blocked")
	}

	// After leak interval, 1 slot leaks
	afterLeak := now.Add(250 * time.Millisecond)
	if res := lb.Allow(afterLeak); !res.Allowed {
		t.Fatalf("Expected request after leak to be allowed")
	}
}

func TestTokenBucket(t *testing.T) {
	capacity := int64(2)
	refillRate := 5.0 // 5 tokens/sec
	tb := NewTokenBucket(capacity, refillRate)

	now := time.Now()
	if res := tb.Allow(now); !res.Allowed {
		t.Fatalf("Expected 1st token allowed")
	}
	if res := tb.Allow(now); !res.Allowed {
		t.Fatalf("Expected 2nd token allowed")
	}
	if res := tb.Allow(now); res.Allowed {
		t.Fatalf("Expected 3rd token blocked immediately")
	}
}

func TestIPRateLimiterMiddleware(t *testing.T) {
	limiter := NewIPRateLimiter(StrategySlidingWindowCounter, 2, 1*time.Second)

	handler := limiter.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))

	// Request 1
	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.1:1234"
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK, got %d", rr.Code)
	}

	// Request 2
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req)
	if rr2.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK, got %d", rr2.Code)
	}

	// Request 3 -> Rate limited
	rr3 := httptest.NewRecorder()
	handler.ServeHTTP(rr3, req)
	if rr3.Code != http.StatusTooManyRequests {
		t.Fatalf("Expected 429 Too Many Requests, got %d", rr3.Code)
	}
}
