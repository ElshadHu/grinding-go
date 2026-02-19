package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

var (
	ErrMaxOpen               = errors.New("max Open must be > 0")
	ErrMaxIdle               = errors.New("max Idle must be <= MaxOpen")
	ErrIdleTimeout           = errors.New("idle Timeout must be > 0")
	ErrFactory               = errors.New("factory mustn't be nil")
	ErrPoolClosed            = errors.New("pool is closed")
	ErrConnectionNil         = errors.New("connection is nil")
	ErrConnectionAlreadyUsed = errors.New("connection already closed")
)

// Config holds the connection pool configuration
type Config struct {
	MaxOpen     int                      // max total connections (idle + in use)
	MaxIdle     int                      // max idle connection kept
	IdleTimeout time.Duration            // close conn if idle longer than this
	Factory     func() (net.Conn, error) // creates new connections
}

// idleConn wraps a connection with metadata for idle timeout checking
type idleConn struct {
	conn       net.Conn
	returnedAt time.Time // for idle timeout check
}

// Pool manages a pool of reusable net.Conn with concurrency liiting via semaphore and LIFO idle connection reuse
type Pool struct {
	mu          sync.Mutex  // protects all shared state
	idleConns   []*idleConn // LIFO stack of idle connections
	idleTimeout time.Duration
	factory     func() (net.Conn, error)
	sem         chan struct{} // buffered channel cap = maxOpen (semaphore)
	closed      bool
	done        chan struct{} // signal to stop cleaner goroutine
	maxOpen     int
	maxIdle     int
}

func NewPool(cfg Config) (*Pool, error) {
	if cfg.MaxOpen <= 0 {
		return nil, ErrMaxOpen
	}
	if cfg.MaxIdle > cfg.MaxOpen {
		return nil, ErrMaxIdle
	}
	if cfg.IdleTimeout <= 0 {
		return nil, ErrIdleTimeout
	}
	if cfg.Factory == nil {
		return nil, ErrFactory
	}
	// all valid - build the pool
	p := &Pool{
		idleConns:   make([]*idleConn, 0),
		idleTimeout: cfg.IdleTimeout,
		factory:     cfg.Factory,
		sem:         make(chan struct{}, cfg.MaxOpen),
		done:        make(chan struct{}),
		maxOpen:     cfg.MaxOpen,
		maxIdle:     cfg.MaxIdle,
	}
	go p.cleanerLoop()
	return p, nil
}

// Get gets a connection from the pool by reusing an idle one or creating a new one via factory
func (p *Pool) Get(ctx context.Context) (*PoolConn, error) {
	select {
	case p.sem <- struct{}{}:
		// got a slot
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	p.mu.Lock()
	for {
		// what id pool closed
		if p.closed {
			p.mu.Unlock()
			<-p.sem
			return nil, ErrPoolClosed
		}
		// Try to pop idle conn  (LIFO = last element)
		n := len(p.idleConns)
		if n == 0 {
			break // no idle conns , fall through to factory
		}
		ic := p.idleConns[n-1]
		// pop it off
		p.idleConns = p.idleConns[:n-1]

		// is it stale?
		if time.Since(ic.returnedAt) > p.idleTimeout {
			// Stale  - close it, try next one
			ic.conn.Close()
			continue // loop back try another one
		}
		// good connection - use it
		p.mu.Unlock()
		return &PoolConn{Conn: ic.conn, pool: p}, nil
	}
	// there is no idle connection create a new one
	p.mu.Unlock()
	conn, err := p.factory()
	if err != nil {
		<-p.sem
		return nil, err
	}
	return &PoolConn{Conn: conn, pool: p}, nil
}

// put puts the connection back to the pool
func (p *Pool) put(conn net.Conn) error {
	if conn == nil {
		return ErrConnectionNil
	}
	p.mu.Lock()
	if p.closed || len(p.idleConns) >= p.maxIdle {
		p.mu.Unlock()
		<-p.sem
		return conn.Close()
	}
	p.idleConns = append(p.idleConns, &idleConn{conn: conn, returnedAt: time.Now()})
	p.mu.Unlock()
	<-p.sem
	return nil
}

// stillValid evicts stale idle conns based on time
func (p *Pool) stillValid() {
	p.mu.Lock()
	defer p.mu.Unlock()
	var stillValid []*idleConn
	for _, ic := range p.idleConns {
		if time.Since(ic.returnedAt) > p.idleTimeout {
			ic.conn.Close()
		} else {
			stillValid = append(stillValid, ic)
		}
	}
	p.idleConns = stillValid

}

// cleanerLoop runs in the background goroutine
func (p *Pool) cleanerLoop() {
	ticker := time.NewTicker(p.idleTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.stillValid()
		case <-p.done:
			return
		}
	}

}

// Close shuts down the pool
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.done) // signals cleanerLoop to exit
	// Close all idle connections
	for _, ic := range p.idleConns {
		ic.conn.Close()
	}
	p.idleConns = nil
}

// PoolConn wraps a net.Conn so that Close() returns the conn to the pool instead of closing it
type PoolConn struct {
	net.Conn
	pool     *Pool
	unusable bool
	closed   bool       // prevents double-close
	mu       sync.Mutex // protects unusable and closed flags
}

// Close returns the conn to the pull or destroys it if it marked unusuable
func (pc *PoolConn) Close() error {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.closed {
		return ErrConnectionAlreadyUsed
	}
	pc.closed = true
	if pc.unusable {
		err := pc.Conn.Close()
		<-pc.pool.sem
		return err
	}
	return pc.pool.put(pc.Conn)
}

func (pc *PoolConn) MarkUnusable() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.unusable = true
}

func main() {
	// Start a local TCP echo server
	listener, err := net.Listen("tcp", "127.0.0.1:0") // :0 random port

	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	fmt.Println("Server on:", listener.Addr())
	// Accept connections in background - echo back whatever is sent
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					c.Write(buf[:n])
				}
			}(conn)

		}
	}()

	// Create the pool

	pool, err := NewPool(Config{
		MaxOpen:     3,
		MaxIdle:     2,
		IdleTimeout: 5 * time.Second,
		Factory: func() (net.Conn, error) {
			return net.Dial("tcp", listener.Addr().String())
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			conn, err := pool.Get(ctx)
			if err != nil {
				fmt.Printf("goroutine %d: failed to get conn: %v\n", id, err)
				return
			}
			// Write to server
			msg := fmt.Sprintf("hello from goroutine %d", id)
			conn.Write([]byte(msg))

			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				fmt.Printf("goroutine %d: read error: %v\n", id, err)
				conn.MarkUnusable()
				conn.Close()
				return
			}
			fmt.Printf("goroutine %d: got echo: %s\n", id, string(buf[:n]))
			conn.Close() // returns to pool
		}(i)
	}
	wg.Wait()
	fmt.Println("all done")

}
