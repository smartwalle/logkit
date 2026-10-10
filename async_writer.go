package logkit

import (
	"errors"
	"io"
	"sync/atomic"

	"github.com/smartwalle/logkit/internal/blockingqueue"
)

type AsyncWriter struct {
	writer io.WriteCloser
	queue  *blockingqueue.Queue[[]byte]

	closed atomic.Bool
	done   chan struct{}

	err atomic.Pointer[error]
}

// NewAsyncWriter 创建异步 Writer。
func NewAsyncWriter(w io.WriteCloser, capacity int) *AsyncWriter {
	var aw = &AsyncWriter{
		writer: w,
		queue:  blockingqueue.New[[]byte](capacity),
		done:   make(chan struct{}),
	}
	go aw.run()
	return aw
}

// Write 将数据复制后加入队列。
// 队列满时阻塞等待；成功返回表示数据已入队，不代表底层写入完成。
func (w *AsyncWriter) Write(p []byte) (int, error) {
	if w.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	if err := w.err.Load(); err != nil {
		return 0, *err
	}

	var data = make([]byte, len(p))
	copy(data, p)

	if err := w.queue.Enqueue(data); err != nil {
		if errors.Is(err, blockingqueue.ErrClosed) {
			return 0, io.ErrClosedPipe
		}
		return 0, err
	}

	return len(p), nil
}

// Close 关闭队列，等待已入队的数据处理完成，并关闭底层 Writer。
// 并发或重复调用均等待后台处理结束。
func (w *AsyncWriter) Close() error {
	if w.closed.CompareAndSwap(false, true) {
		w.queue.Close()
	}

	<-w.done
	if err := w.err.Load(); err != nil {
		return *err
	}
	return nil
}

// run 按顺序将队列中的数据写入底层 Writer。
func (w *AsyncWriter) run() {
	defer func() {
		if err := w.writer.Close(); err != nil {
			w.err.CompareAndSwap(nil, &err)
		}
		close(w.done)
	}()

	for {
		data, err := w.queue.Dequeue()
		if errors.Is(err, blockingqueue.ErrClosed) {
			return
		}
		if err != nil {
			w.err.CompareAndSwap(nil, &err)
			return
		}

		n, err := w.writer.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}

		if err != nil {
			w.err.CompareAndSwap(nil, &err)
		}
	}
}
