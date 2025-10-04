package log

import (
	"io"
)

type LogWriter struct {
	writer       io.WriterAt // Underlying writer (usually a file)
	currentLSN   LSN         // Next LSN to assign
	flushedLSN   LSN         // Last LSN guaranteed on disk
	buffer       []byte      // Write buffer
	bufferOffset int         // Current position in buffer
	bufferSize   int         // Maximum buffer size
}

// NewLogWriter creates a new LogWriter with the given underlying writer and buffer size
func NewLogWriter(writer io.WriterAt, bufferSize int, current, flushed LSN) *LogWriter {
	return &LogWriter{
		writer:       writer,
		bufferSize:   bufferSize,
		buffer:       make([]byte, bufferSize),
		bufferOffset: 0,
		currentLSN:   current,
		flushedLSN:   flushed,
	}

}

// Write appends data to the buffer and returns the LSN
// This is where we implement the actual buffering strategy
func (w *LogWriter) Write(data []byte) (LSN, error) {
	assignedLSN := w.currentLSN

	if len(data) > w.bufferSize {

		if err := w.flush(); err != nil {
			return 0, err
		}

		_, err := w.writer.WriteAt(data, int64(w.flushedLSN))
		if err != nil {
			return 0, err
		}

		w.flushedLSN += LSN(len(data))
		w.currentLSN += LSN(len(data))
		return assignedLSN, nil
	}

	if w.bufferOffset+len(data) > w.bufferSize {
		if err := w.flush(); err != nil {
			return 0, err
		}
	}
	copy(w.buffer[w.bufferOffset:], data)
	w.bufferOffset += len(data)
	w.currentLSN += LSN(len(data))

	return assignedLSN, nil
}

// Force ensures data is on disk up to the given LSN
// This is called during commit to guarantee durability
func (w *LogWriter) Force(lsn LSN) error {
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

func (w *LogWriter) CurrentLSN() LSN {
	return w.currentLSN
}

func (w *LogWriter) Close() error {
	return w.flush()
}
