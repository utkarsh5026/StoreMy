package wal

import (
	"io"
	"storemy/pkg/primitives"
)

type LogWriter struct {
	writer       io.WriterAt
	currentLSN   primitives.LSN
	flushedLSN   primitives.LSN
	buffer       []byte
	bufferOffset int
	bufferSize   int
}

// NewLogWriter creates a new LogWriter with the given underlying writer and buffer size
func NewLogWriter(writer io.WriterAt, bufferSize int, current, flushed primitives.LSN) *LogWriter {
	return &LogWriter{
		writer:       writer,
		bufferSize:   bufferSize,
		buffer:       make([]byte, bufferSize),
		bufferOffset: 0,
		currentLSN:   current,
		flushedLSN:   flushed,
	}

}

// Write appends data to the buffer and returns the primitives.LSN
// This is where we implement the actual buffering strategy
func (w *LogWriter) Write(data []byte) (primitives.LSN, error) {
	assignedLSN := w.currentLSN

	if len(data) > w.bufferSize {

		if err := w.flush(); err != nil {
			return 0, err
		}

		_, err := w.writer.WriteAt(data, int64(w.flushedLSN))
		if err != nil {
			return 0, err
		}

		bytesWritten := primitives.LSN(len(data))
		w.flushedLSN += bytesWritten
		w.currentLSN += bytesWritten
		return assignedLSN, nil
	}

	if w.bufferOffset+len(data) > w.bufferSize {
		if err := w.flush(); err != nil {
			return 0, err
		}
	}
	copy(w.buffer[w.bufferOffset:], data)
	w.bufferOffset += len(data)
	w.currentLSN += primitives.LSN(len(data))

	return assignedLSN, nil
}

// Force ensures data is on disk up to the given primitives.LSN
// This is called during commit to guarantee durability
func (w *LogWriter) Force(lsn primitives.LSN) error {
	if w.flushedLSN >= lsn {
		return nil
	}

	return w.flush()
}

// flush writes the buffer to the underlying writer
// This is an internal method, not part of the interface
func (w *LogWriter) flush() error {
	if w.bufferOffset == 0 {
		return nil
	}

	_, err := w.writer.WriteAt(w.buffer[:w.bufferOffset], int64(w.flushedLSN))
	if err != nil {
		return err
	}

	w.flushedLSN = w.currentLSN
	w.bufferOffset = 0
	return nil
}

func (w *LogWriter) CurrentLSN() primitives.LSN {
	return w.currentLSN
}

func (w *LogWriter) Close() error {
	return w.flush()
}
