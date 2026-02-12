package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrTopicAlreadyExists      = errors.New("topic already exists")
	ErrTopicNotFound           = errors.New("topic not found")
	ErrSubscriberNotSubscribed = errors.New("Subscriber not subscribed")
	ErrSubscriberAlreadyExists = errors.New("Subscriber already exists")
	ErrNilSubscriber           = errors.New("SubscriberNotExists")
)

// Message is just the data
type Message struct {
	body      string
	topic     string
	timestamp time.Time
}

// Subscriber is the one receives messages. It tells Yo Broker, I want to listen to topic X
// and then passivelt receives via its channel
type Subscriber struct {
	id string
	ch chan Message // it doesn't have to be a pointer
}

func NewSubscriber(id string) *Subscriber {
	return &Subscriber{
		id: id,
		ch: make(chan Message, 100), // buffered so publisher doesn't block
	}
}

// Topic is a category/label that groups related messages
type Topic struct {
	name        string
	subscribers map[string]*Subscriber
	mu          sync.RWMutex // multiple readers, single writer
}

func NewTopic(name string) *Topic {
	return &Topic{
		name:        name,
		subscribers: make(map[string]*Subscriber),
	}
}

// AddSubscriber adds a subscriber to this topic
// Why: Topic owns its subscriber map, so it should manage additions
// This methdo grabs a Write lock on Topic.mu.
func (t *Topic) AddSubscriber(sub *Subscriber) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if sub == nil {
		return ErrNilSubscriber
	}
	if _, ok := t.subscribers[sub.id]; ok {
		return ErrSubscriberAlreadyExists
	}
	t.subscribers[sub.id] = sub
	return nil
}

// RemoveSubscriber removes a subscriber from this topic
// Why: Clean Removal, also grabs Write lock
func (t *Topic) RemoveSubscriber(subID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.subscribers[subID]; !ok {
		return ErrSubscriberNotSubscribed
	}
	// if I don't close that shi I will just delete from map the goroutine from that channel will block forever
	sub := t.subscribers[subID]
	close(sub.ch)
	delete(t.subscribers, subID)
	return nil
}

// Broadcast sends a message to all subscribers of this topic
func (t *Topic) Broadcast(msg Message) error {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, sub := range t.subscribers {
		// yo I need to  use subscriber as a parameter because in another case it is closure bug
		// Because  by the time goroutine runs, the for loop may have moved and sub points to the last sub
		go func(s *Subscriber) {
			select {
			case s.ch <- msg:
			default:
			}
		}(sub)
	}
	return nil
}

// Broker knows which subscribers care about which topics and route messages to them
type Broker struct {
	topics map[string]*Topic
	mu     sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		topics: make(map[string]*Topic),
	}
}

// CreateTopic creates a new topic in the broker
// Why: because it is needed a way to register topics befoe pub/sub
// if does not exist it needs to return an error
func (b *Broker) CreateTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[name]; ok {
		return ErrTopicAlreadyExists
	}
	b.topics[name] = NewTopic(name) // Create a real Topic not nil
	return nil
}

// Subscriber registers a subscriber to a topic
// if topic does not exist especially in rest APIs it returns an error
// if subscriber id is already in use in that case it will use the existing one
func (b *Broker) Subscribe(topicName string, sub *Subscriber) error {
	b.mu.Lock()
	t, ok := b.topics[topicName]
	b.mu.Unlock() // release broker lock before touching topic
	if !ok {
		return ErrTopicNotFound
	}
	return t.AddSubscriber(sub) // topic handles its own locking
}

// Unsubscribe removes a subscriber from a topic
// Why: Subscribers need to be able to stop receiving messages
func (b *Broker) Unsubscribe(topicName string, sub *Subscriber) error {
	b.mu.Lock()
	t, ok := b.topics[topicName]
	b.mu.Unlock() // release broker lock before touching topic
	if !ok {
		return ErrTopicNotFound
	}
	return t.RemoveSubscriber(sub.id)
}

func (b *Broker) Publish(topicName string, msg Message) error {
	b.mu.Lock()
	t, ok := b.topics[topicName]
	b.mu.Unlock()
	if !ok {
		return ErrTopicNotFound
	}
	return t.Broadcast(msg)

}

func main() {
	// Create broker
	broker := NewBroker()
	// Create topic
	broker.CreateTopic("weather")
	//Create subscribers
	sub1 := NewSubscriber("user-1")
	sub2 := NewSubscriber("user-2")
	broker.Subscribe("weather", sub1)
	broker.Subscribe("weather", sub2)
	// start listening goroutines before publishing
	// each subscriber needs a goroutine reading from its channel
	var wg sync.WaitGroup
	wg.Add(2) // expecting 2 subscribers to receive
	go func() {
		defer wg.Done()
		msg := <-sub1.ch
		fmt.Printf("sub1 received: %s\n", msg.body)
	}()
	go func() {
		defer wg.Done()
		msg := <-sub2.ch
		fmt.Printf("sub2 received: %s\n", msg.body)
	}()

	broker.Publish("weather", Message{
		body:      "it is cool",
		topic:     "weather",
		timestamp: time.Now(),
	})
	wg.Wait()

}
