package main

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrLongURLNotExist   = errors.New("long url does not exist")
	ErrURLAlreadyExist   = errors.New("url already exist")
	ErrShortAlreadyExist = errors.New("short already exist")
)

const base62Chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type URLShortener struct {
	shortToLong map[string]string
	// I need reverse lookup for checking that shi is shortened before or not
	// otherwise it would take O(n) we can sacrifice memory here :(
	longToShort map[string]string
	mu          sync.RWMutex
	counter     int64
}

func NewURLShortener() *URLShortener {
	return &URLShortener{
		shortToLong: make(map[string]string),
		longToShort: make(map[string]string),
		counter:     0,
	}
}

func (us *URLShortener) Shorten(longURL string) (string, error) {
	us.mu.Lock()
	defer us.mu.Unlock()
	if len(longURL) == 0 {
		return "", ErrLongURLNotExist
	}
	// Check duplicate first before incrementing counter
	if _, ok := us.longToShort[longURL]; ok {
		return "", ErrShortAlreadyExist
	}
	us.counter++
	shortCode := Encode(us.counter)
	us.shortToLong[shortCode] = longURL
	us.longToShort[longURL] = shortCode
	return shortCode, nil
}

func (us *URLShortener) Resolve(shortCode string) (string, error) {
	us.mu.RLock()
	defer us.mu.RUnlock()
	lr, ok := us.shortToLong[shortCode]
	if !ok {
		return "", ErrLongURLNotExist
	}
	return lr, nil
}

func (us *URLShortener) Remove(shortCode string) error {
	us.mu.Lock()
	defer us.mu.Unlock()
	lr, ok := us.shortToLong[shortCode]
	if !ok {
		return ErrLongURLNotExist
	}
	delete(us.shortToLong, shortCode)
	delete(us.longToShort, lr)
	return nil
}

// Encode converts a counterID into a compact Base62 short code
// It divides the number repeatedly by 62 and maps each remainder to an alpanum chat, then reverse the result
func Encode(counterID int64) string {
	if counterID == 0 {
		return string(base62Chars[0])
	}
	b := []byte{}
	for counterID > 0 {
		b = append(b, base62Chars[counterID%62])
		counterID /= 62
	}
	// here we are going to reverse that shi via 2 pointers
	left := 0
	right := len(b) - 1
	for left < right {
		b[left], b[right] = b[right], b[left]
		left++
		right--
	}
	return string(b)
}

func main() {
	urlShortener := NewURLShortener()
	shortCode, err := urlShortener.Shorten("https://google.com")
	fmt.Println("Short code:", shortCode, "Error:", err)
	longURL, err := urlShortener.Resolve(shortCode)
	fmt.Println("Resolved:", longURL, "Error:", err)
	// Duplicate test
	_, err = urlShortener.Shorten("https://google.com")
	fmt.Println("Duplicate error:", err)
	// Concurrent Test
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			url := fmt.Sprintf("https:google.com/%d", id)
			code, err := urlShortener.Shorten(url)
			fmt.Printf("Goroutine %d: code=%s err=%v\n", id, code, err)
		}(i)
	}
	wg.Wait()
	// Delete test
	err = urlShortener.Remove(shortCode)
	fmt.Println("Deleted:", err)
	_, err = urlShortener.Resolve(shortCode)
	fmt.Println("After delete:", err)

}
