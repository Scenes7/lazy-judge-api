package main

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// ─── Token-bucket rate limiter ────────────────────────────────────────────────
//
// Two independent rate-limit pools:
//   - "fetch"  — for GET /problem/:id and GET /api/problems/random
//   - "submit" — for POST /submit
//
// Each pool is keyed by the client's real IP, extracted from:
//   1. CF-Connecting-IP  (Cloudflare Orange-Cloud)
//   2. X-Real-IP         (generic reverse-proxy header)
//   3. First entry of X-Forwarded-For (CloudFront / any L7 proxy)
//   4. c.ClientIP()      (Gin default, falls back to RemoteAddr)

// bucket is a single token bucket for one IP.
type bucket struct {
	tokens    float64
	capacity  float64
	refillPer float64   // tokens added per second
	lastFill  time.Time
	pending   int       // number of goroutines waiting in the queue
}

// take refills based on elapsed time, then attempts to remove one token.
func (b *bucket) take() bool {
	now := time.Now()
	elapsed := now.Sub(b.lastFill).Seconds()
	b.tokens += elapsed * b.refillPer
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.lastFill = now

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// limiterPool holds all buckets for one rate-limit class.
type limiterPool struct {
	mu        sync.Mutex
	buckets   map[string]*bucket
	capacity  float64
	refillPer float64 // tokens per second
}

func newPool(capacity float64, refillPer float64) *limiterPool {
	p := &limiterPool{
		buckets:   make(map[string]*bucket),
		capacity:  capacity,
		refillPer: refillPer,
	}
	go p.gc()
	return p
}

// allow checks (and decrements) the bucket for the given key.
func (p *limiterPool) allow(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	b, ok := p.buckets[key]
	if !ok {
		b = &bucket{
			tokens:    p.capacity, // start full
			capacity:  p.capacity,
			refillPer: p.refillPer,
			lastFill:  time.Now(),
		}
		p.buckets[key] = b
	}
	return b.take()
}

// waitForToken tries to take a token immediately. If none are available and
// fewer than maxQueue requests are already queued for this key, the caller
// blocks until a token refills (up to timeout). Returns true if a token was
// eventually granted, false if the queue is full or the timeout expired.
func (p *limiterPool) waitForToken(key string, maxQueue int, timeout time.Duration) bool {
	p.mu.Lock()

	b, ok := p.buckets[key]
	if !ok {
		b = &bucket{
			tokens:    p.capacity,
			capacity:  p.capacity,
			refillPer: p.refillPer,
			lastFill:  time.Now(),
		}
		p.buckets[key] = b
	}

	// Fast path: token available right now.
	if b.take() {
		p.mu.Unlock()
		return true
	}

	// Queue is full — reject immediately.
	if b.pending >= maxQueue {
		p.mu.Unlock()
		return false
	}

	b.pending++
	p.mu.Unlock()

	// Slow path: poll for a token, sleeping precisely based on the deficit.
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		if b.take() {
			b.pending--
			p.mu.Unlock()
			return true
		}
		// Calculate how long until the next token arrives.
		deficit := 1.0 - b.tokens
		waitSec := deficit / b.refillPer
		p.mu.Unlock()

		sleepDur := time.Duration(waitSec * float64(time.Second))
		if remaining := time.Until(deadline); sleepDur > remaining {
			sleepDur = remaining
		}
		if sleepDur > 0 {
			time.Sleep(sleepDur)
		}
	}

	// Timed out — remove ourselves from the queue.
	p.mu.Lock()
	b.pending--
	p.mu.Unlock()
	return false
}

// gc removes buckets that have been idle (and would be full) for over 10 min.
// This prevents unbounded memory growth from many unique IPs.
func (p *limiterPool) gc() {
	ticker := time.NewTicker(5 * time.Minute)
	for range ticker.C {
		p.mu.Lock()
		cutoff := time.Now().Add(-10 * time.Minute)
		for k, b := range p.buckets {
			if b.lastFill.Before(cutoff) {
				delete(p.buckets, k)
			}
		}
		p.mu.Unlock()
	}
}

// ─── Client IP extraction (CloudFront + Cloudflare aware) ─────────────────────

func realIP(c *gin.Context) string {
	// 1. Cloudflare always sets this to the true client IP.
	if ip := c.GetHeader("CF-Connecting-IP"); ip != "" {
		return ip
	}
	// 2. Some proxies (nginx) set X-Real-IP.
	if ip := c.GetHeader("X-Real-IP"); ip != "" {
		return ip
	}
	// 3. X-Forwarded-For: client, proxy1, proxy2 — first entry is client.
	if xff := c.GetHeader("X-Forwarded-For"); xff != "" {
		if parts := strings.SplitN(xff, ",", 2); len(parts) > 0 {
			ip := strings.TrimSpace(parts[0])
			if ip != "" {
				return ip
			}
		}
	}
	// 4. Gin default (usually RemoteAddr) — strip port.
	ip := c.ClientIP()
	if host, _, err := net.SplitHostPort(ip); err == nil {
		return host
	}
	return ip
}

// ─── Gin middleware factories ─────────────────────────────────────────────────

// rateLimitMiddleware returns a Gin middleware that enforces a token-bucket
// limit per IP. Requests that exceed the limit receive 429 + a JSON body.
func rateLimitMiddleware(pool *limiterPool) gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := realIP(c)
		if !pool.allow(ip) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":  "rate_limited",
				"detail": "Too many requests — please slow down.",
			})
			return
		}
		c.Next()
	}
}
