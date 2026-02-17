package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// Global logger instance and synchronization
var (
	Logger   *slog.Logger
	loggerMu sync.RWMutex
	logFile  *os.File // Track file handle for cleanup
	isInited bool
	initOnce sync.Once // For lazy initialization in GetLogger
)

// LogLevel represents logging verbosity
type LogLevel string

const (
	LevelDebug LogLevel = "DEBUG"
	LevelInfo  LogLevel = "INFO"
	LevelWarn  LogLevel = "WARN"
	LevelError LogLevel = "ERROR"
)

// Config holds logger configuration
type Config struct {
	Level      LogLevel
	OutputPath string // Empty for stdout, or file path
	Format     string // "json" or "text"
}

// Init initializes the global logger with the given configuration.
// This should be called once at application startup.
// Subsequent calls to Init will return an error to prevent multiple initialization.
//
// Example:
//
//	logging.Init(logging.Config{
//	    Level: logging.LevelInfo,
//	    OutputPath: "logs/database.log",
//	    Format: "json",
//	})
func Init(config Config) error {
	loggerMu.Lock()
	defer loggerMu.Unlock()

	if isInited {
		return fmt.Errorf("logger already initialized; call Close() first to reinitialize")
	}

	var writer io.Writer

	if config.OutputPath == "" {
		writer = os.Stdout
	} else {
		logDir := filepath.Dir(config.OutputPath)
		if err := os.MkdirAll(logDir, 0o750); err != nil {
			return err
		}

		file, err := os.OpenFile(config.OutputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		writer = file
		logFile = file
	}

	var level slog.Level
	switch config.Level {
	case LevelDebug:
		level = slog.LevelDebug
	case LevelInfo:
		level = slog.LevelInfo
	case LevelWarn:
		level = slog.LevelWarn
	case LevelError:
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}

	if config.Format == "json" {
		handler = slog.NewJSONHandler(writer, opts)
	} else {
		handler = slog.NewTextHandler(writer, opts)
	}

	Logger = slog.New(handler)
	isInited = true
	return nil
}

// InitDefault initializes the logger with sensible defaults:
// - Level: INFO
// - Output: stdout
// - Format: text
// This is safe to call multiple times and will only initialize once.
func InitDefault() {
	loggerMu.Lock()
	defer loggerMu.Unlock()

	if isInited {
		return
	}

	Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	isInited = true
}

// Close closes the logger and any open file handles.
// After calling Close, you can call Init again to reinitialize.
// It's safe to call Close multiple times.
func Close() error {
	loggerMu.Lock()
	defer loggerMu.Unlock()

	if !isInited {
		return nil
	}

	var err error
	if logFile != nil {
		err = logFile.Close()
		logFile = nil
	}

	Logger = nil
	isInited = false

	initOnce = sync.Once{}
	return err
}

// GetLogger returns the current logger instance in a thread-safe manner.
// If the logger is not initialized, it initializes with defaults using sync.Once
// for efficient lazy initialization.
func GetLogger() *slog.Logger {
	loggerMu.RLock()
	if isInited {
		logger := Logger
		loggerMu.RUnlock()
		return logger
	}
	loggerMu.RUnlock()

	initOnce.Do(func() {
		InitDefault()
	})

	loggerMu.RLock()
	logger := Logger
	loggerMu.RUnlock()
	return logger
}

// Debug logs a debug message in a thread-safe manner
func Debug(msg string, args ...any) {
	GetLogger().Debug(msg, args...)
}

// Info logs an info message in a thread-safe manner
func Info(msg string, args ...any) {
	GetLogger().Info(msg, args...)
}

// Warn logs a warning message in a thread-safe manner
func Warn(msg string, args ...any) {
	GetLogger().Warn(msg, args...)
}

// Error logs an error message in a thread-safe manner
func Error(msg string, args ...any) {
	GetLogger().Error(msg, args...)
}
