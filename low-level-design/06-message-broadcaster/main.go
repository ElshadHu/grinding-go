package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrBroadcastFull = errors.New("broadcast channel full")
)

// Represents a connected entity
type Client struct {
	hub        *Hub
	send       chan []byte // messages waiting to be delivered to this client
	disconnect sync.Once   // prevent double disconnect
	registered chan struct{}
}

func NewClient(hub *Hub) *Client {
	return &Client{
		hub:        hub,
		send:       make(chan []byte, 256),
		registered: make(chan struct{}),
	}
}

func (c *Client) Connect(ctx context.Context) error {
	select {
	case c.hub.register <- c:
		// sent to hub
	case <-ctx.Done():
		return ctx.Err()
	}

	// Wait for hub to confirm registration
	select {
	case <-c.registered:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Disconnect unregisters the client and even if multiple times disconnect is called
// unregister will happen once
func (c *Client) Disconnect() {
	c.disconnect.Do(func() {
		select {
		case c.hub.unregister <- c:
			// sent
		default:
			// hub is busy or dead don't block
		}
	})
}

func (c *Client) Send(message []byte) error {
	select {
	case c.hub.broadcast <- message:
		return nil
	default:
		return ErrBroadcastFull

	}
}

// Listen is waiting for messages to arrive
func (c *Client) Listen(wg *sync.WaitGroup) {
	defer wg.Done()
	for message := range c.send {
		fmt.Printf("Client received %s\n", message)
	}
	// if we are here, c.send was closed
	fmt.Println("Client disconnected")
}

type Hub struct {
	clients    map[*Client]bool // registered clients
	broadcast  chan []byte      // incoming messages to fan out
	register   chan *Client     // client wants to join
	unregister chan *Client     // client wants to leave
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) handleRegister(c *Client) {
	if _, ok := h.clients[c]; ok {
		return
	}
	h.clients[c] = true
	close(c.registered)
}

func (h *Hub) handleUnregister(c *Client) {
	if _, ok := h.clients[c]; !ok {
		return
	}
	delete(h.clients, c)
	close(c.send)
}
func (h *Hub) handleBroadcast(message []byte) {
	for c := range h.clients {
		select {
		case c.send <- message:
		// Delivered
		default:
			// slow client, evict
			close(c.send)
			delete(h.clients, c)
		}
	}
}

func (h *Hub) shutdown() {
	for {
		select {
		case message := <-h.broadcast:
			h.handleBroadcast(message)
		default:
			for c := range h.clients {
				delete(h.clients, c)
				close(c.send)
			}
			return
		}
	}
}
func (h *Hub) Run(ctx context.Context) error {
	for {
		select {

		case <-ctx.Done():
			h.shutdown()
			return ctx.Err()
		case client := <-h.register:
			h.handleRegister(client)
		case client := <-h.unregister:
			h.handleUnregister(client)
		case message := <-h.broadcast:
			h.handleBroadcast(message)
		}
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	hub := NewHub()
	go hub.Run(ctx)
	client1 := NewClient(hub)
	client2 := NewClient(hub)
	client3 := NewClient(hub)
	// Register with hub first
	if err := client1.Connect(ctx); err != nil {
		fmt.Printf("client1 failed to connect: %v\n", err)
		return
	}
	if err := client2.Connect(ctx); err != nil {
		fmt.Printf("client2 failed to connect: %v\n", err)
		return
	}
	if err := client3.Connect(ctx); err != nil {
		fmt.Printf("client3 failed to connect: %v\n", err)
		return
	}

	var listenWg sync.WaitGroup
	listenWg.Add(3)
	// start listening in goroutines
	go client1.Listen(&listenWg)
	go client2.Listen(&listenWg)
	go client3.Listen(&listenWg)
	client1.Send([]byte("Hello from client 1"))
	client2.Send([]byte("Hello from client 2"))
	client3.Send([]byte("Hello from client 3"))

	client3.Disconnect()
	cancel()
	listenWg.Wait()

}
