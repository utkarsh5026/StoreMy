package logging

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

// Global logger instance
var Logger *slog.Logger

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
//
// Example:
//
//	logging.Init(logging.Config{
//	    Level: logging.LevelInfo,
//	    OutputPath: "logs/database.log",
//	    Format: "json",
//	})
func Init(config Config) error {
	var writer io.Writer
	var err error

	// Determine output destination
	if config.OutputPath == "" {
		writer = os.Stdout
	} else {
		// Ensure log directory exists
		logDir := filepath.Dir(config.OutputPath)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return err
		}

		writer, err = os.OpenFile(config.OutputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
	}

	// Parse log level
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

	// Create handler based on format
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}

	if config.Format == "json" {
		handler = slog.NewJSONHandler(writer, opts)
	} else {
		handler = slog.NewTextHandler(writer, opts)
	}

	Logger = slog.New(handler)
	return nil
}

// InitDefault initializes the logger with sensible defaults:
// - Level: INFO
// - Output: stdout
// - Format: text
func InitDefault() {
	Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

// Debug logs a debug message
func Debug(msg string, args ...any) {
	if Logger != nil {
		Logger.Debug(msg, args...)
	}
}

// Info logs an info message
func Info(msg string, args ...any) {
	if Logger != nil {
		Logger.Info(msg, args...)
	}
}

// Warn logs a warning message
func Warn(msg string, args ...any) {
	if Logger != nil {
		Logger.Warn(msg, args...)
	}
}

// Error logs an error message
func Error(msg string, args ...any) {
	if Logger != nil {
		Logger.Error(msg, args...)
	}
}
