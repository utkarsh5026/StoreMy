// Package logging provides a process-wide structured logger for StoreMy.
//
// The package wraps [log/slog] and exposes a single global logger instance
// that is initialized once and then retrieved via GetLogger. All subsystems
// should obtain a logger through this package rather than constructing their
// own slog.Logger values, so that log level and output destination are
// controlled from a single place.
//
// # Initialisation
//
// Call Init (or InitDefault for sensible defaults) once at program startup,
// before any goroutines that might call GetLogger are spawned:
//
//	if err := logging.Init(logging.LevelDebug, "/var/log/storemy/wal.log"); err != nil {
//	    log.Fatal(err)
//	}
//
// InitDefault writes INFO-level logs to stderr without a log file.
//
// # Retrieving the logger
//
//	logger := logging.GetLogger()
//	logger.Info("database opened", "name", dbName)
//
// If GetLogger is called before Init, a default stderr logger is created
// lazily (via sync.Once) so that packages that log during init are safe.
//
// # Context helpers
//
// Several helpers return child loggers pre-populated with structured fields,
// reducing repetition in hot paths:
//
//	log := logging.WithTx(txID)      // adds tx_id field
//	log := logging.WithTable(name)   // adds table field
//	log := logging.WithOp(opName)    // adds op field
package logging
