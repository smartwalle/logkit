// Package bufferedwriter 提供带大小和定时刷新的并发安全 io.Writer。
package bufferedwriter

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/smartwalle/logkit"
)

// Writer 将数据暂存于内存，并在缓冲区满、定时器触发或关闭时写入底层 Writer。
//
// Writer 可被多个 goroutine 并发调用。Close 会刷新缓冲区，并在底层 Writer
// 实现 io.Closer 时关闭它。
type Writer struct {
	mu sync.Mutex

	writer  io.Writer
	buffer  []byte
	options options

	workerStop chan struct{}
	workerDone chan struct{}
	closeDone  chan struct{}
	closed     bool
	runtimeErr error
	closeErr   error
}

// New 创建一个包装 writer 的缓冲 Writer。
func New(writer io.Writer, opts ...Option) (*Writer, error) {
	if writer == nil {
		return nil, fmt.Errorf("writer must not be nil")
	}
	nOpts, err := resolveOptions(opts)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		writer:     writer,
		buffer:     make([]byte, 0, nOpts.bufferSize),
		options:    nOpts,
		workerDone: make(chan struct{}),
		closeDone:  make(chan struct{}),
	}
	if w.options.flushInterval > 0 {
		w.workerStop = make(chan struct{})
		go w.runFlushWorker()
	} else {
		close(w.workerDone)
	}
	return w, nil
}

// Write 将 p 作为完整数据单元写入内存缓冲区，不会将一次 Write 拆分为多次
// 底层写入。缓冲区满时会同步刷新到底层 Writer；首个写入或刷新错误会被记录，
// 并由 Close 返回。
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, logkit.ErrWriterClosed
	}
	if w.runtimeErr != nil {
		return 0, w.runtimeErr
	}
	if len(p) == 0 {
		return 0, nil
	}
	if len(p) > w.options.bufferSize {
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
		return w.writeLocked(p)
	}
	if len(w.buffer)+len(p) > w.options.bufferSize {
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
	}
	w.buffer = append(w.buffer, p...)
	if len(w.buffer) == w.options.bufferSize {
		if err := w.flushLocked(); err != nil {
			return len(p), err
		}
	}
	return len(p), nil
}

// Close 停止定时刷新、刷新缓冲区并关闭底层 Writer。
// Close 可重复调用；每次调用均返回相同的最终错误。
func (w *Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		done := w.closeDone
		w.mu.Unlock()
		<-done
		w.mu.Lock()
		err := w.closeErr
		w.mu.Unlock()
		return err
	}
	w.closed = true
	if w.workerStop != nil {
		close(w.workerStop)
	}
	w.mu.Unlock()
	<-w.workerDone

	w.mu.Lock()
	_ = w.flushLocked()
	var closeErr error
	if closer, ok := w.writer.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			closeErr = fmt.Errorf("close underlying writer: %w", err)
		}
	}
	w.closeErr = errors.Join(w.runtimeErr, closeErr)
	result := w.closeErr
	close(w.closeDone)
	w.mu.Unlock()
	return result
}

func (w *Writer) runFlushWorker() {
	ticker := time.NewTicker(w.options.flushInterval)
	defer ticker.Stop()
	defer close(w.workerDone)
	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if !w.closed {
				_ = w.flushLocked()
			}
			w.mu.Unlock()
		case <-w.workerStop:
			return
		}
	}
}

func (w *Writer) flushLocked() error {
	if len(w.buffer) == 0 {
		return nil
	}
	n, err := w.writeLocked(w.buffer)
	if n > 0 {
		copy(w.buffer, w.buffer[n:])
		w.buffer = w.buffer[:len(w.buffer)-n]
	}
	return err
}

func (w *Writer) writeLocked(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if n < 0 || n > len(p) {
		err = fmt.Errorf("write buffered log: invalid write count %d", n)
		n = 0
	} else if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		err = fmt.Errorf("write buffered log: %w", err)
		w.recordRuntimeErrorLocked(err)
	}
	return n, err
}

func (w *Writer) recordRuntimeErrorLocked(err error) {
	if err != nil && w.runtimeErr == nil {
		w.runtimeErr = err
	}
}
