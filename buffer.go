package logkit

import (
	"bytes"
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
// 并关闭底层 writer。
type Buffer struct {
	mu sync.Mutex

	writer io.Writer
	buffer bytes.Buffer

	bufferSize    int
	flushInterval time.Duration

	workerStop chan struct{}
	workerDone chan struct{}
	closed     bool
	err        error
}

// NewBuffer 创建一个包装 writer 的 Buffer。
func NewBuffer(w io.Writer, bufferSize int, flushInterval time.Duration) (*Buffer, error) {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}

	var b = &Buffer{
		writer:        w,
		bufferSize:    bufferSize,
		flushInterval: flushInterval,
		workerDone:    make(chan struct{}),
	}
	b.buffer.Grow(b.bufferSize)

	if b.flushInterval > 0 {
		b.workerStop = make(chan struct{})
		go b.runFlushWorker()
	} else {
		close(b.workerDone)
	}

	return b, nil
}

// Write 将 p 作为完整数据单元追加到内存缓冲区。
// 当缓冲区空间不足时，会先同步刷新已有数据；当 p 大于缓冲区容量时，
// 则在刷新已有数据后直接写入底层 writer。
// 首次底层写入或刷新错误会被记录，后续 Write 将返回该错误。
func (b *Buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return 0, io.ErrClosedPipe
	}
	if b.err != nil {
		return 0, b.err
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

	_, _ = b.buffer.Write(p)

	if b.buffer.Len() == b.bufferSize {
		if err := b.flushLocked(); err != nil {
			return len(p), err
		}
	}

	return len(p), nil
}

// Close 停止定时刷新、刷新缓冲区并关闭底层 writer。
// 重复调用直接返回 nil。
func (b *Buffer) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}

	b.closed = true
	if b.workerStop != nil {
		close(b.workerStop)
	}
	b.mu.Unlock()

	<-b.workerDone

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.err != nil {
		return b.err
	}

	if err := b.flushLocked(); err != nil {
		return err
	}
	return nil
}

func (b *Buffer) runFlushWorker() {
	var ticker = time.NewTicker(b.flushInterval)
	defer ticker.Stop()
	defer close(b.workerDone)

	for {
		select {
		case <-ticker.C:
			b.mu.Lock()
			if b.closed || b.err != nil {
				b.mu.Unlock()
				return
			}

			var err = b.flushLocked()
			b.mu.Unlock()

			if err != nil {
				return
			}

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

	if n < 0 || n > len(p) || (err == nil && n != len(p)) {
		n = 0
		err = io.ErrShortWrite
	}

	if err != nil && b.err == nil {
		b.err = err
	}

	return n, err
}
