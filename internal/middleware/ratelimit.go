package middleware

import (
	"encoding/json"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type StrategyType string

const (
	StrategySlidingWindowCounter StrategyType = "sliding_window"
	StrategyLeakyBucket          StrategyType = "leaky_bucket"
	StrategyTokenBucket          StrategyType = "token_bucket"
)

type RateLimitResult struct {
	Allowed   bool
	Limit     int64
	Remaining int64
	ResetIn   time.Duration
}

type Strategy interface {
	Allow(now time.Time) RateLimitResult
}

// ---------------------------------------------------------
// 1. Sliding Window Counter Strategy
// ---------------------------------------------------------
type SlidingWindowCounter struct {
	mu          sync.Mutex
	windowSize  time.Duration
	limit       int64
	prevCount   int64
	currCount   int64
	windowStart time.Time
}

func NewSlidingWindowCounter(limit int64, windowSize time.Duration) *SlidingWindowCounter {
	return &SlidingWindowCounter{
		limit:       limit,
		windowSize:  windowSize,
		windowStart: time.Now(),
	}
}

func (s *SlidingWindowCounter) Allow(now time.Time) RateLimitResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if now.Sub(s.windowStart) >= s.windowSize {
		windowsPassed := now.Sub(s.windowStart) / s.windowSize
		if windowsPassed == 1 {
			s.prevCount = s.currCount
		} else {
			s.prevCount = 0
		}
		s.currCount = 0
		s.windowStart = s.windowStart.Add(windowsPassed * s.windowSize)
	}

	timeIntoCurrentWindow := now.Sub(s.windowStart)
	weight := 1.0 - (float64(timeIntoCurrentWindow) / float64(s.windowSize))
	if weight < 0 {
		weight = 0
	}

	estimatedCount := int64(math.Floor(float64(s.prevCount)*weight)) + s.currCount
	remainingWindowTime := s.windowSize - timeIntoCurrentWindow

	if estimatedCount < s.limit {
		s.currCount++
		remaining := s.limit - (estimatedCount + 1)
		if remaining < 0 {
			remaining = 0
		}
		return RateLimitResult{
			Allowed:   true,
			Limit:     s.limit,
			Remaining: remaining,
			ResetIn:   remainingWindowTime,
		}
	}

	return RateLimitResult{
		Allowed:   false,
		Limit:     s.limit,
		Remaining: 0,
		ResetIn:   remainingWindowTime,
	}
}

// ---------------------------------------------------------
// 2. Leaky Bucket Strategy
// ---------------------------------------------------------
type LeakyBucket struct {
	mu           sync.Mutex
	capacity     int64
	leakInterval time.Duration
	waterLevel   int64
	lastLeak     time.Time
}

func NewLeakyBucket(capacity int64, leakInterval time.Duration) *LeakyBucket {
	return &LeakyBucket{
		capacity:     capacity,
		leakInterval: leakInterval,
		lastLeak:     time.Now(),
	}
}

func (l *LeakyBucket) Allow(now time.Time) RateLimitResult {
	l.mu.Lock()
	defer l.mu.Unlock()

	elapsed := now.Sub(l.lastLeak)
	leaked := int64(elapsed / l.leakInterval)
	if leaked > 0 {
		l.waterLevel -= leaked
		if l.waterLevel < 0 {
			l.waterLevel = 0
		}
		l.lastLeak = l.lastLeak.Add(time.Duration(leaked) * l.leakInterval)
	}

	if l.waterLevel < l.capacity {
		l.waterLevel++
		remaining := l.capacity - l.waterLevel
		return RateLimitResult{
			Allowed:   true,
			Limit:     l.capacity,
			Remaining: remaining,
			ResetIn:   l.leakInterval,
		}
	}

	return RateLimitResult{
		Allowed:   false,
		Limit:     l.capacity,
		Remaining: 0,
		ResetIn:   l.leakInterval,
	}
}

// ---------------------------------------------------------
// 3. Token Bucket Strategy
// ---------------------------------------------------------
type TokenBucket struct {
	mu         sync.Mutex
	capacity   int64
	tokens     float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

func NewTokenBucket(capacity int64, refillRate float64) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		tokens:     float64(capacity),
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

func (t *TokenBucket) Allow(now time.Time) RateLimitResult {
	t.mu.Lock()
	defer t.mu.Unlock()

	elapsed := now.Sub(t.lastRefill).Seconds()
	t.tokens += elapsed * t.refillRate
	if t.tokens > float64(t.capacity) {
		t.tokens = float64(t.capacity)
	}
	t.lastRefill = now

	if t.tokens >= 1.0 {
		t.tokens -= 1.0
		remaining := int64(math.Floor(t.tokens))
		return RateLimitResult{
			Allowed:   true,
			Limit:     t.capacity,
			Remaining: remaining,
			ResetIn:   time.Duration((1.0 / t.refillRate) * float64(time.Second)),
		}
	}

	missing := 1.0 - t.tokens
	neededSec := missing / t.refillRate
	return RateLimitResult{
		Allowed:   false,
		Limit:     t.capacity,
		Remaining: 0,
		ResetIn:   time.Duration(neededSec * float64(time.Second)),
	}
}

// ---------------------------------------------------------
// Telemetry & Rate Limiter Manager
// ---------------------------------------------------------
type IPRateLimiter struct {
	mu           sync.RWMutex
	strategyType StrategyType
	limit        int64
	window       time.Duration
	clients      map[string]Strategy
	lastSeen     map[string]time.Time

	totalRequests   int64
	allowedRequests int64
	blockedRequests int64
}

func NewIPRateLimiter(strategy StrategyType, limit int64, window time.Duration) *IPRateLimiter {
	limiter := &IPRateLimiter{
		strategyType: strategy,
		limit:        limit,
		window:       window,
		clients:      make(map[string]Strategy),
		lastSeen:     make(map[string]time.Time),
	}

	go limiter.cleanupStaleIPs(2 * time.Minute)

	return limiter
}

func (limiter *IPRateLimiter) SetStrategy(strategy StrategyType, limit int64, window time.Duration) {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	limiter.strategyType = strategy
	limiter.limit = limit
	limiter.window = window
	limiter.clients = make(map[string]Strategy)
	limiter.lastSeen = make(map[string]time.Time)
}

func (limiter *IPRateLimiter) createStrategyLocked() Strategy {
	switch limiter.strategyType {
	case StrategyLeakyBucket:
		leakInterval := limiter.window / time.Duration(limiter.limit)
		if leakInterval <= 0 {
			leakInterval = 100 * time.Millisecond
		}
		return NewLeakyBucket(limiter.limit, leakInterval)
	case StrategyTokenBucket:
		refillRate := float64(limiter.limit) / limiter.window.Seconds()
		if refillRate <= 0 {
			refillRate = 1.0
		}
		return NewTokenBucket(limiter.limit, refillRate)
	case StrategySlidingWindowCounter:
		fallthrough
	default:
		return NewSlidingWindowCounter(limiter.limit, limiter.window)
	}
}

func (limiter *IPRateLimiter) getClientStrategy(ip string) Strategy {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	limiter.lastSeen[ip] = time.Now()
	strat, exists := limiter.clients[ip]
	if !exists {
		strat = limiter.createStrategyLocked()
		limiter.clients[ip] = strat
	}
	return strat
}

func (limiter *IPRateLimiter) Allow(ip string) RateLimitResult {
	atomic.AddInt64(&limiter.totalRequests, 1)
	strat := limiter.getClientStrategy(ip)
	res := strat.Allow(time.Now())

	if res.Allowed {
		atomic.AddInt64(&limiter.allowedRequests, 1)
	} else {
		atomic.AddInt64(&limiter.blockedRequests, 1)
	}

	return res
}

func (limiter *IPRateLimiter) GetMetrics() map[string]interface{} {
	limiter.mu.RLock()
	defer limiter.mu.RUnlock()

	return map[string]interface{}{
		"active_strategy":  string(limiter.strategyType),
		"limit":            limiter.limit,
		"window_seconds":   limiter.window.Seconds(),
		"total_requests":   atomic.LoadInt64(&limiter.totalRequests),
		"allowed_requests": atomic.LoadInt64(&limiter.allowedRequests),
		"blocked_requests": atomic.LoadInt64(&limiter.blockedRequests),
		"active_clients":   len(limiter.clients),
	}
}

func (limiter *IPRateLimiter) cleanupStaleIPs(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		limiter.mu.Lock()
		now := time.Now()
		for ip, last := range limiter.lastSeen {
			if now.Sub(last) > 5*time.Minute {
				delete(limiter.clients, ip)
				delete(limiter.lastSeen, ip)
			}
		}
		limiter.mu.Unlock()
	}
}

func (limiter *IPRateLimiter) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := extractIP(r)
		res := limiter.Allow(ip)

		w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(res.Limit, 10))
		w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(res.Remaining, 10))
		resetSec := int64(math.Ceil(res.ResetIn.Seconds()))
		if resetSec < 1 {
			resetSec = 1
		}
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetSec, 10))

		if !res.Allowed {
			w.Header().Set("Retry-After", strconv.FormatInt(resetSec, 10))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error":       "Too Many Requests",
				"status":      http.StatusTooManyRequests,
				"message":     "Rate limit exceeded. Please try again later.",
				"retry_after": resetSec,
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

func extractIP(r *http.Request) string {
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		return forwarded
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}
