package main

import (
	"fmt"
	"sync"
	"time"
)

type RateLimiter struct {
	buckets   map[string]*TokenBucket // maps a user/key to their bucket
	mu        sync.Mutex              // Protects concurrent map access
	rate      float64                 // default rate for new buckets
	maxTokens float64                 //default burst size for new buckets
}

func NewRateLimiter(rate, maxTokens float64) *RateLimiter {
	rl := &RateLimiter{
		buckets:   make(map[string]*TokenBucket),
		rate:      rate,
		maxTokens: maxTokens,
	}
	go rl.cleanUpLoop(30*time.Second, 5*time.Minute)
	return rl
}

func (rl *RateLimiter) cleanUpLoop(interval, maxIdle time.Duration) {
	ticker := time.NewTicker(interval)

	for range ticker.C {
		rl.mu.Lock()
		// you can't do that shi with defer here cause  it will not run at the end of each loop iteration
		for key, tb := range rl.buckets {
			if time.Since(tb.lastSeen) > maxIdle {
				delete(rl.buckets, key)
			}
		}
		rl.mu.Unlock()
	}
}

func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	if tb, exists := rl.buckets[key]; exists {
		return tb.Allow()
	}
	tb := NewTokenBucket(rl.rate, rl.maxTokens)
	rl.buckets[key] = tb
	return tb.Allow()
}

type TokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64   // how many tokes get added per second
	lastRefill time.Time // the last time we added to the bucket
	lastSeen   time.Time // the last time we checked the bucket
}

func NewTokenBucket(rate float64, maxTokens float64) *TokenBucket {
	return &TokenBucket{
		tokens:     maxTokens,
		maxTokens:  maxTokens,
		refillRate: rate,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	tb.lastSeen = now
	newTokens := now.Sub(tb.lastRefill).Seconds() * tb.refillRate
	tb.tokens += newTokens
	if tb.tokens > tb.maxTokens {
		tb.tokens = tb.maxTokens
	}
	tb.lastRefill = now
	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

func main() {
	rl := NewRateLimiter(2, 3)
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			allowed := rl.Allow("user1")
			fmt.Printf("Request: %2d: %v\n", id, allowed)
		}(i)
	}
	wg.Wait()

	fmt.Println("user-2: 3 requests")
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			allowed := rl.Allow("user2")
			fmt.Printf("Request: %2d: %v\n", id, allowed)
		}(i)
	}
	wg.Wait()

	fmt.Println("user-1: after 1s sleep (refill)")
	time.Sleep(1 * time.Second)
	fmt.Printf("user-1 after sleep: %v\n", rl.Allow("user1"))
}
