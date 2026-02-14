package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

var (
	ErrKeyNotFound        = errors.New("Key not found")
	ErrValueAlreadyExists = errors.New("Value already exists")
)

// entry holds a value and its optional expiration time
type entry struct {
	value     string
	expiresAt time.Time // zero value = no expiration
}

// Storage defines the interface for a key-value store
type Storage interface {
	Set(key, value string) error
	SetWithTTL(key, value string, ttl time.Duration) error
	Get(key string) (string, error)
	Delete(key string) error
}

// MapStorage is a thread-safe in-memory key-value store with TTL support
type MapStorage struct {
	mu       sync.Mutex
	keyValue map[string]entry
}

// NewMapStorage creates a new MapStorage and starts a background cleanup goroutine
func NewMapStorage(ctx context.Context, cleanUpInterval time.Duration) *MapStorage {
	ms := &MapStorage{
		keyValue: make(map[string]entry),
	}
	go ms.cleanupLoop(ctx, cleanUpInterval)
	return ms
}

// Set stores permanently
func (m *MapStorage) Set(key, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.keyValue[key]
	if ok {
		return ErrValueAlreadyExists
	}
	m.keyValue[key] = entry{value: value} // expiresAt is zero  = never expires
	return nil
}

// SetWithTTL stores with expires after  the given ttl duration
func (m *MapStorage) SetWithTTL(key, value string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.keyValue[key]
	if ok {
		return ErrValueAlreadyExists
	}
	m.keyValue[key] = entry{value: value, expiresAt: time.Now().Add(ttl)}
	return nil
}

// Get retrieves the value for a key. The real gang here is that when it is read it is lazly deleted
func (m *MapStorage) Get(key string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.keyValue[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		delete(m.keyValue, key)
		return "", ErrKeyNotFound
	}
	return e.value, nil
}

// Delete removes a key from the store
func (m *MapStorage) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.keyValue[key]
	if !ok {
		return ErrKeyNotFound
	}
	delete(m.keyValue, key)
	return nil
}

// evictExpired scans all keys and removes any that have passed their expiration time
func (m *MapStorage) evictExpired() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for k, e := range m.keyValue {
		if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
			delete(m.keyValue, k)
		}
	}

}

// cleanupLoop runs in a bacground goroutine, calling evictExpired at each interval
func (m *MapStorage) cleanupLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.evictExpired()
		case <-ctx.Done():
			return
		}
	}

}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mapStore := NewMapStorage(ctx, 5*time.Second)
	var wg sync.WaitGroup

	// Concurrent writes with ttl - 3 second expiry
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", id)
			err := mapStore.SetWithTTL(key, strconv.Itoa(id), 3*time.Second)
			if err != nil {
				fmt.Printf("SetWithTTL error for %s: %v\n", key, err)
			} else {
				fmt.Printf("SetWithTTL %s (expires in 3s)\n", key)
			}
		}(i)
	}

	wg.Wait()
	// Concurrent Reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", id)
			val, err := mapStore.Get(key)
			if err != nil {
				fmt.Printf("Get error for %s: %v\n", key, err)
			} else {
				fmt.Printf("Got %s = %s\n", key, val)
			}
		}(i)
	}
	wg.Wait()

	//  Concurrent writes without ttl - permanent
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("perm_key%d", id)
			err := mapStore.Set(key, strconv.Itoa(id))
			if err != nil {
				fmt.Printf("Set error for %s: %v\n", key, err)
			} else {
				fmt.Printf("Set %s (permanent)\n", key)
			}
		}(i)
	}
	wg.Wait()

	// Concurrent deletes
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", id)
			err := mapStore.Delete(key)
			if err != nil {
				fmt.Printf("Delete error for %s: %v\n", key, err)
			} else {
				fmt.Printf("Deleted: %s\n", key)
			}
		}(i)
	}
	wg.Wait()

	//  these should return ErrKeyNotFound
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		_, err := mapStore.Get(key)
		fmt.Printf("After delete, Get %s: %v\n", key, err)
	}

	// Wait for TTL to expire
	fmt.Println("\n Sleeping 4 seconds for TTL to expire ")
	time.Sleep(4 * time.Second)

	// TTL keys should now be expired via lazy expiration
	fmt.Println("\nReading after 4s (remaining TTL keys should be expired)")
	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		_, err := mapStore.Get(key)
		fmt.Printf("Get %s: %v\n", key, err)
	}

	// Permanent keys should still exist
	fmt.Println("\nPermanent keys should still exist")
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("perm_key%d", i)
		val, err := mapStore.Get(key)
		fmt.Printf("Get %s: val=%s, err=%v\n", key, val, err) // should still work
	}
}
