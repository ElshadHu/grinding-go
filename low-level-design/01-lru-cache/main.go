package main

import (
	"errors"
	"fmt"
	"sync"
)

type Node struct {
	key   int
	value int
	prev  *Node
	next  *Node
}

type LinkedList struct {
	head *Node
	tail *Node
}

func (ll *LinkedList) insertAtHead(key, value int) *Node {
	newNode := &Node{key: key, value: value}
	if ll.head == nil {
		ll.head = newNode
		ll.tail = newNode
		return newNode
	}
	newNode.next = ll.head
	ll.head.prev = newNode
	ll.head = newNode
	return newNode
}

func (ll *LinkedList) removeNode(n *Node) error {
	if n == nil {
		return errors.New("node is nil")
	}
	// Update previous node's next pointer
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		// n is head
		ll.head = n.next
	}
	// update next node's prev pointer
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		// n is the tail
		ll.tail = n.prev
	}
	// clear dangling pointers
	n.prev = nil
	n.next = nil
	return nil
}

func (ll *LinkedList) moveToHead(n *Node) error {
	if n == nil {
		return errors.New("node is nil")
	}
	// if already head , do nothing
	if ll.head == n {
		return nil
	}
	// then we insert at the beginning of the list
	err := ll.removeNode(n)
	if err != nil {
		return err
	}
	if ll.head == nil {
		ll.head = n
		ll.tail = n
		n.prev = nil
		n.next = nil
		return nil
	}
	n.next = ll.head
	ll.head.prev = n
	n.prev = nil
	ll.head = n
	return nil
}

// removeTail which is going to be used in least recently used node and returns its key
//
//	that is why this is gang shi
func (ll *LinkedList) removeTail() (int, error) {
	if ll.tail == nil {
		return -1, errors.New("there is not a tail")
	}

	// store the key before removing
	tailKey := ll.tail.key

	// if there is only one node
	if ll.tail.prev == nil {
		ll.head = nil
		ll.tail = nil
		return tailKey, nil
	}

	// remove tail and update
	ll.tail = ll.tail.prev
	ll.tail.next = nil
	return tailKey, nil
}

type LRUCache struct {
	mu       sync.RWMutex
	capacity int
	cache    map[int]*Node
	list     *LinkedList
}

// Initialize cache with given capacity
func NewLRUCache(capacity int) (*LRUCache, error) {
	if capacity <= 0 {
		return nil, errors.New("capacity should be bigger than 0")
	}

	return &LRUCache{
		capacity: capacity,
		cache:    make(map[int]*Node),
		list:     &LinkedList{},
	}, nil

}

// Get the value associated to the key, return -1 and error if not found
func (lru *LRUCache) Get(key int) int {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	node, exists := lru.cache[key]
	if !exists {
		return -1
	}
	err := lru.list.moveToHead(node)
	if err != nil {
		return -1
	}
	return node.value
}

// Insert or update key-value pair
func (lru *LRUCache) Put(key int, value int) error {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	if node, exists := lru.cache[key]; exists {
		node.value = value
		return lru.list.moveToHead(node)
	}

	// so if it doesn't exist we need to create it
	newNode := lru.list.insertAtHead(key, value)
	lru.cache[key] = newNode

	// Check if it passes the capacity
	if len(lru.cache) > lru.capacity {
		tailKey, err := lru.list.removeTail()
		if err != nil {
			return err
		}
		delete(lru.cache, tailKey)
	}
	return nil
}

func main() {
	cache, err := NewLRUCache(3)
	if err != nil {
		fmt.Println("Error creating cache: ", err)
		return
	}
	// WaitGroup to synchronize goroutines
	var wg sync.WaitGroup
	var mu sync.Mutex // for printing safely

	// Sequential put operations
	cache.Put(7, 1)
	cache.Put(8, 2)
	cache.Put(9, 3)

	fmt.Println("Concurrent Puts")
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := cache.Put(id, id*10)
			if err != nil {
				mu.Lock()
				fmt.Printf("Goroutine %d - Put error: %v\n", id, err)
				mu.Unlock()
			}
			val := cache.Get(id)
			mu.Lock()
			fmt.Printf("Get(%d) = %d\n", id, val)
			mu.Unlock()
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	fmt.Println("After concurrent operations")

	for i := 0; i < 5; i++ {
		val := cache.Get(i)
		fmt.Printf("Get(%d) = %d\n", i, val)
	}

}
