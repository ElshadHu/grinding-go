package main

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

var (
	ErrEmptySlice      = errors.New("that is an empty slice")
	ErrServerExists    = errors.New("server already exists")
	ErrServerNotExists = errors.New("it is not possible to remove the server because it doesn't exist")
	ErrRingEmpty       = errors.New("ring is empty")
)

type Hash func(data []byte) uint32

type HashRing struct {
	mu       sync.RWMutex
	hashFunc Hash
	replicas int
	keys     []int
	hashMap  map[int]string
}

func NewHashRing(replicaCount int, fn Hash) *HashRing {
	hr := &HashRing{
		hashFunc: fn,
		replicas: replicaCount,
		keys:     make([]int, 0),
		hashMap:  make(map[int]string),
	}
	if hr.hashFunc == nil {
		hr.hashFunc = crc32.ChecksumIEEE // for hashing strings, this is being used
	}
	return hr
}

// AddNode hashes each server replicas times and inserts virtual nodes
func (hr *HashRing) AddNode(serverNames []string) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	if len(serverNames) == 0 {
		return ErrEmptySlice
	}
	for _, sn := range serverNames {
		for i := 0; i < hr.replicas; i++ {
			hashed := hr.hashFunc([]byte(strconv.Itoa(i) + sn))
			if _, ok := hr.hashMap[int(hashed)]; ok {
				return ErrServerExists
			}
			hr.hashMap[int(hashed)] = sn
			hr.keys = append(hr.keys, int(hashed))
		}
	}
	// Sort Ints is only working with int :( therefore I did casting
	sort.Ints(hr.keys)
	return nil
}

// RemoveNode removes all virtual nodes for that server, rebuilds the sorted keys
func (hr *HashRing) RemoveNode(serverName string) error {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	for i := 0; i < hr.replicas; i++ {
		hashed := hr.hashFunc([]byte(strconv.Itoa(i) + serverName))
		if _, ok := hr.hashMap[int(hashed)]; !ok {
			return ErrServerNotExists
		}
		delete(hr.hashMap, int(hashed))
	}
	hr.keys = make([]int, 0, len(hr.hashMap))
	for k := range hr.hashMap {
		hr.keys = append(hr.keys, k)
	}
	sort.Ints(hr.keys)
	return nil
}

// GetNode Hashes the key, binary searches the ring for the next server
func (hr *HashRing) GetNode(key string) (string, error) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	if len(hr.keys) == 0 {
		return "", ErrRingEmpty
	}
	hashed := hr.hashFunc([]byte(key))
	left := 0
	right := len(hr.keys)
	for left < right {
		middle := left + (right-left)/2
		if hr.keys[middle] < int(hashed) {
			left = middle + 1
		} else {
			right = middle
		}
	}

	if left == len(hr.keys) {
		left = 0
	}
	return hr.hashMap[hr.keys[left]], nil
}

// IsEmpty is for checking is there any node on the ring
func (hr *HashRing) IsEmpty() bool {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	if len(hr.keys) == 0 {
		return true
	}
	return false
}

func main() {
	hr := NewHashRing(3, nil)
	hr.AddNode([]string{"server-A", "server-B", "server-C"})

	for _, key := range []string{"user:1", "user:2", "order:99", "session:abc"} {
		server, _ := hr.GetNode(key)
		fmt.Printf("%s → %s\n", key, server)
	}

	before, _ := hr.GetNode("user:1")
	hr.RemoveNode("server-B")
	after, _ := hr.GetNode("user:1")
	fmt.Printf("user:1 before: %s, after: %s\n", before, after)
}
