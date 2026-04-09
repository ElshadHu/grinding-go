package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Core types

type Request struct {
	ID      string
	Payload any
	// each request carries its own response channel
	ResCh chan Response
}

type Response struct {
	RequestID string
	ServerID  string
	Err       error
}

type Server struct {
	ID      string
	Address string
}

type RoundRobin struct {
	// for atomic ops
	counter uint64
}

func (rr *RoundRobin) Next(servers []*Server) *Server {
	idx := atomic.AddUint64(&rr.counter, 1)

	return servers[idx%uint64(len(servers))]
}

// Load Balancer
type LoadBalancer struct {
	servers   []*Server
	rr        *RoundRobin
	requestCh chan Request
	doneCh    chan struct{}
	mu        sync.RWMutex
	wg        sync.WaitGroup
}

func NewLoadBalancer() *LoadBalancer {
	return &LoadBalancer{
		servers:   make([]*Server, 0),
		rr:        &RoundRobin{},
		requestCh: make(chan Request, 100),
		doneCh:    make(chan struct{}),
	}
}

func (lb *LoadBalancer) AddServer(s *Server) {
	lb.mu.Lock()
	lb.servers = append(lb.servers, s)
	lb.mu.Unlock()
}

func (lb *LoadBalancer) RemoveServer(id string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	for i := 0; i < len(lb.servers); i++ {
		if lb.servers[i].ID == id {
			lb.servers[i] = lb.servers[len(lb.servers)-1]
			lb.servers = lb.servers[:len(lb.servers)-1]
			return
		}
	}
}

// Send request to into requestCh
func (lb *LoadBalancer) Submit(req Request) {
	lb.requestCh <- req
}

func (lb *LoadBalancer) dispatch() {
	for {
		select {
		case req := <-lb.requestCh:
			lb.mu.RLock()
			server := lb.rr.Next(lb.servers)
			lb.mu.RUnlock()
			lb.wg.Add(1)
			go lb.forward(server, req)
		case <-lb.doneCh:
			return
		}
	}

}

func (lb *LoadBalancer) forward(s *Server, req Request) {
	defer lb.wg.Done()
	time.Sleep(100 * time.Millisecond)
	req.ResCh <- Response{
		RequestID: req.ID,
		ServerID:  s.ID,
	}
}

func (lb *LoadBalancer) Shutdown() {
	close(lb.doneCh)
	lb.wg.Wait()
}

func main() {
	lb := NewLoadBalancer()
	lb.AddServer(&Server{ID: "s1", Address: ":8081"})
	lb.AddServer(&Server{ID: "s2", Address: ":8082"})
	lb.AddServer(&Server{ID: "s3", Address: ":8083"})
	go lb.dispatch()

	for i := 0; i < 10; i++ {
		resCh := make(chan Response, 1)
		lb.Submit(Request{ID: fmt.Sprintf("req-%d", i), ResCh: resCh})
		go func(ch chan Response) {
			res := <-ch
			fmt.Printf("[%s] handled by %s\n", res.RequestID, res.ServerID)
		}(resCh)
	}

	time.Sleep(2 * time.Second)
	lb.Shutdown()
}
