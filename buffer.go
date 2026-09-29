package logkit

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	defaultBufferSize = 32 << 10
)

// Buffer 将数据暂存于内存，并在缓冲区满、定时器触发或关闭时写入底层 writer.
//
// Buffer 可被多个 goroutine 并发调用。底层 writer 首次写入失败后，Buffer
// 将进入失败状态，后续 Write 返回该错误。Close 会停止定时刷新、刷新剩余数据，
// 并在底层 writer 实现 io.Closer 时关闭它。
type Buffer struct {
	mu sync.Mutex

	writer io.Writer
	buffer bytes.Buffer

	bufferSize    int
	flushInterval time.Duration

	workerStop chan struct{}
	workerDone chan struct{}
	closeDone  chan struct{}
	closed     bool
	runtimeErr error
	closeErr   error
}

// NewBuffer 创建一个包装 writer 的 Buffer。
func NewBuffer(writer io.Writer, bufferSize int, flushInterval time.Duration) (*Buffer, error) {
	if writer == nil {
		return nil, fmt.Errorf("writer must not be nil")
	}
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}

	w := &Buffer{
		writer:        writer,
		bufferSize:    bufferSize,
		flushInterval: flushInterval,
		workerDone:    make(chan struct{}),
		closeDone:     make(chan struct{}),
	}
	w.buffer.Grow(w.bufferSize)
	if w.flushInterval > 0 {
		w.workerStop = make(chan struct{})
		go w.runFlushWorker()
	} else {
		close(w.workerDone)
	}
	return w, nil
}

// Write 将 p 作为完整数据单元追加到内存缓冲区。
// 当缓冲区空间不足时，会先同步刷新已有数据；当 p 大于缓冲区容量时，
// 则在刷新已有数据后直接写入底层 writer。
// 首次底层写入或刷新错误会被记录，后续 Write 将返回该错误，并由 Close 返回。
func (b *Buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, ErrWriterClosed
	}
	if b.runtimeErr != nil {
		return 0, b.runtimeErr
	}
	if len(p) == 0 {
		return 0, nil
	}
	if len(p) > b.bufferSize {
		if err := b.flushLocked(); err != nil {
			return 0, err
		}
		return b.writeLocked(p)
	}
	if b.buffer.Len()+len(p) > b.bufferSize {
		if err := b.flushLocked(); err != nil {
			return 0, err
		}
	}
	b.buffer.Write(p)
	if b.buffer.Len() == b.bufferSize {
		if err := b.flushLocked(); err != nil {
			return len(p), err
		}
	}
	return len(p), nil
}

// Close 停止定时刷新、刷新缓冲区并关闭底层 Buffer。
// Close 可重复调用；每次调用均返回相同的最终错误。
func (b *Buffer) Close() error {
	b.mu.Lock()
	if b.closed {
		done := b.closeDone
		b.mu.Unlock()
		<-done
		b.mu.Lock()
		err := b.closeErr
		b.mu.Unlock()
		return err
	}
	b.closed = true
	if b.workerStop != nil {
		close(b.workerStop)
	}
	b.mu.Unlock()
	<-b.workerDone

	b.mu.Lock()
	_ = b.flushLocked()
	var closeErr error
	if closer, ok := b.writer.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			closeErr = fmt.Errorf("close underlying writer: %w", err)
		}
	}
	b.closeErr = errors.Join(b.runtimeErr, closeErr)
	result := b.closeErr
	close(b.closeDone)
	b.mu.Unlock()
	return result
}

func (b *Buffer) runFlushWorker() {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()
	defer close(b.workerDone)
	for {
		select {
		case <-ticker.C:
			b.mu.Lock()
			if !b.closed {
				_ = b.flushLocked()
			}
			b.mu.Unlock()
		case <-b.workerStop:
			return
		}
	}
}

func (b *Buffer) flushLocked() error {
	if b.buffer.Len() == 0 {
		return nil
	}
	n, err := b.writeLocked(b.buffer.Bytes())
	if n > 0 {
		b.buffer.Next(n)
	}
	return err
}

func (b *Buffer) writeLocked(p []byte) (int, error) {
	n, err := b.writer.Write(p)
	if n < 0 || n > len(p) {
		err = fmt.Errorf("write buffered log: invalid write count %d", n)
		n = 0
	} else if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		err = fmt.Errorf("write buffered log: %w", err)
		b.recordRuntimeErrorLocked(err)
	}
	return n, err
}

func (b *Buffer) recordRuntimeErrorLocked(err error) {
	if err != nil && b.runtimeErr == nil {
		b.runtimeErr = err
	}
}
