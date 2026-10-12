package bufferwriter

import (
	"bytes"
	"io"
	"sync"
	"time"
)

const (
	defaultBufferSize = 32 << 10
)

// Writer 将数据暂存于内存，并在缓冲区满、定时器触发或关闭时写入底层 writer.
//
// Writer 可被多个 goroutine 并发调用。底层 writer 首次写入失败后，Buffer
// 将进入失败状态，后续 Write 返回该错误。Close 会停止定时刷新、刷新剩余数据，
// 并关闭底层 writer。
type Writer struct {
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

// New 创建一个包装 writer 的 Writer。
func New(w io.Writer, bufferSize int, flushInterval time.Duration) (*Writer, error) {
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}

	var b = &Writer{
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
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, io.ErrClosedPipe
	}
	if w.err != nil {
		return 0, w.err
	}
	if len(p) == 0 {
		return 0, nil
	}

	if len(p) > w.bufferSize {
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
		return w.writeLocked(p)
	}

	if w.buffer.Len()+len(p) > w.bufferSize {
		if err := w.flushLocked(); err != nil {
			return 0, err
		}
	}

	_, _ = w.buffer.Write(p)

	if w.buffer.Len() == w.bufferSize {
		if err := w.flushLocked(); err != nil {
			return len(p), err
		}
	}

	return len(p), nil
}

// Close 停止定时刷新、刷新缓冲区并关闭底层 writer。
// 重复调用直接返回 nil。
func (w *Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}

	w.closed = true
	if w.workerStop != nil {
		close(w.workerStop)
	}
	w.mu.Unlock()

	<-w.workerDone

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return w.err
	}

	if err := w.flushLocked(); err != nil {
		return err
	}
	return nil
}

func (w *Writer) runFlushWorker() {
	var ticker = time.NewTicker(w.flushInterval)
	defer ticker.Stop()
	defer close(w.workerDone)

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if w.closed || w.err != nil {
				w.mu.Unlock()
				return
			}

			var err = w.flushLocked()
			w.mu.Unlock()

			if err != nil {
				return
			}

		case <-w.workerStop:
			return
		}
	}
}

func (w *Writer) flushLocked() error {
	if w.buffer.Len() == 0 {
		return nil
	}

	n, err := w.writeLocked(w.buffer.Bytes())
	if n > 0 {
		w.buffer.Next(n)
	}

	return err
}

func (w *Writer) writeLocked(p []byte) (int, error) {
	n, err := w.writer.Write(p)

	if n < 0 || n > len(p) || (err == nil && n != len(p)) {
		n = 0
		err = io.ErrShortWrite
	}

	if err != nil && w.err == nil {
		w.err = err
	}

	return n, err
}
