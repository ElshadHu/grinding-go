package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// Level represents log severity
type Level int

const (
	INFO Level = iota
	WARN
	ERROR
	FATAL
)

type LogEntry struct {
	Level     Level
	Message   string
	Timestamp time.Time
}

func (l Level) String() string {
	switch l {
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}

}

type Handler interface {
	Write(entry *LogEntry) error
	Close() error
}

// Concrete Handler
type ConsoleHandler struct {
	mu sync.Mutex
}

func (c *ConsoleHandler) Write(entry *LogEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := fmt.Fprintf(os.Stdout, "[%s] [%s] [%s]\n", entry.Timestamp.Format(time.RFC3339), entry.Level.String(), entry.Message)
	return err
}

func (c *ConsoleHandler) Close() error {
	return nil
}

// FilHandler to write to the disk
type FileHandler struct {
	file *os.File
	mu   sync.Mutex
}

func NewFileHandler(path string) (*FileHandler, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &FileHandler{
		file: file}, nil
}

func (f *FileHandler) Write(entry *LogEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, err := fmt.Fprintf(f.file, "[%s] [%s] [%s]\n", entry.Timestamp.Format(time.RFC3339), entry.Level.String(), entry.Message)
	return err
}

func (f *FileHandler) Close() error {
	return f.file.Close()
}

type Logger struct {
	handlers map[Level]Handler
	minLevel Level
	mu       sync.Mutex
}

func NewLogger(handlers map[Level]Handler, minLevel Level) *Logger {
	return &Logger{
		handlers: handlers,
		minLevel: minLevel,
	}
}
func (l *Logger) log(level Level, msg string) {
	if level < l.minLevel {
		return
	}
	l.mu.Lock()
	handler, ok := l.handlers[level]
	if !ok {
		return
	}
	l.mu.Unlock()
	entry := &LogEntry{
		Level:     level,
		Message:   msg,
		Timestamp: time.Now(),
	}
	handler.Write(entry)
}
func (l *Logger) Info(msg string) {
	l.log(INFO, msg)
}

func (l *Logger) Warn(msg string) {
	l.log(WARN, msg)
}

func (l *Logger) Error(msg string) {
	l.log(ERROR, msg)
}

func (l *Logger) Fatal(msg string) {
	l.log(FATAL, msg)
	os.Exit(1)
}

func (l *Logger) Close() {
	for _, h := range l.handlers {
		h.Close()
	}
}

func main() {
	warnHandler, _ := NewFileHandler("warn.log")
	errorHandler, _ := NewFileHandler("error.log")
	fatalHandler, _ := NewFileHandler("fatal.log")
	logger := NewLogger(map[Level]Handler{
		INFO:  &ConsoleHandler{},
		WARN:  warnHandler,
		ERROR: errorHandler,
		FATAL: fatalHandler,
	}, INFO)
	var wg sync.WaitGroup
	numGoRoutines := 100
	for i := 0; i < numGoRoutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			logger.Info(fmt.Sprintf("Goroutine %d: info message", id))
			logger.Warn(fmt.Sprintf("Goroutine %d: warning message", id))
			logger.Error(fmt.Sprintf("Goroutine %d: error message", id))
		}(i)
	}
	wg.Wait()

}
