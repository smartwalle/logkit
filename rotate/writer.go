// Package rotate 提供并发安全的日志文件轮转 Writer。
package rotate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ErrClosed 表示 Writer 已关闭，不能再执行写入操作。
var ErrClosed = errors.New("writer is closed")

// Writer 是一个向文件追加日志并按大小或时间自动轮转的 io.Writer。
//
// Writer 可被多个 goroutine 并发调用；它独占管理指定的日志路径，不与外部
// logrotate 混用。
type Writer struct {
	mu sync.Mutex

	filename string
	file     *os.File
	size     int64
	options  options

	rotatedAt  time.Time
	nextRotate time.Time
	sequence   uint64
	closed     bool

	maintenance    chan struct{}
	workerDone     chan struct{}
	closeDone      chan struct{}
	runtimeErr     error
	maintenanceErr error
	closeErr       error
}

// NewWriter 创建 filename 的父目录（如有必要）并以追加模式打开日志文件。
func NewWriter(filename string, opts ...Option) (*Writer, error) {
	if !validFilename(filename) {
		return nil, fmt.Errorf("filename must not be empty")
	}
	var err error
	nOpts, err := resolveOptions(opts)
	if err != nil {
		return nil, err
	}
	w := &Writer{
		filename: filename, options: nOpts,
		maintenance: make(chan struct{}, 1), workerDone: make(chan struct{}), closeDone: make(chan struct{}),
	}
	if err = w.openLocked(); err != nil {
		return nil, err
	}
	if err = removeTemporaryFiles(filename); err != nil {
		_ = w.file.Close()
		return nil, err
	}
	go w.runMaintenance()
	if w.needsMaintenance() {
		w.enqueueMaintenance()
	}
	return w, nil
}

// Write 将 p 写入当前日志文件，并与其他 Write 调用串行执行。
//
// 写入前会根据大小和时间策略自动轮转。首个写入或轮转错误会被记录，并由 Close
// 返回；轮转失败但当前文件仍可写时，Write 会完成当前写入并返回轮转错误。
func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	if err := w.ensureFileLocked(); err != nil {
		w.recordRuntimeErrorLocked(err)
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	var rotationErr error
	if w.shouldRotateLocked(int64(len(p))) {
		if err := w.rotateLocked(); err != nil {
			// 轮转失败时，优先继续使用已恢复打开的当前日志文件。
			if w.file == nil {
				w.recordRuntimeErrorLocked(err)
				return 0, err
			}
			rotationErr = err
		}
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	if err != nil {
		writeErr := fmt.Errorf("write log file: %w", err)
		result := errors.Join(rotationErr, writeErr)
		w.recordRuntimeErrorLocked(result)
		return n, result
	}
	if n != len(p) {
		writeErr := errors.New("write log file: short write")
		result := errors.Join(rotationErr, writeErr)
		w.recordRuntimeErrorLocked(result)
		return n, result
	}
	if rotationErr != nil {
		w.recordRuntimeErrorLocked(rotationErr)
		return n, rotationErr
	}
	return n, nil
}

// Close 停止维护任务、等待已排队的压缩和清理完成，并关闭日志文件。
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
	close(w.maintenance)
	w.mu.Unlock()
	<-w.workerDone

	w.mu.Lock()
	var closeErr error
	if w.file != nil {
		closeErr = w.file.Close()
		w.file = nil
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close log file: %w", closeErr)
	}
	w.closeErr = errors.Join(w.runtimeErr, w.maintenanceErr, closeErr)
	result := w.closeErr
	close(w.closeDone)
	w.mu.Unlock()
	return result
}

func (w *Writer) recordRuntimeErrorLocked(err error) {
	if err != nil && w.runtimeErr == nil {
		w.runtimeErr = err
	}
}

func (w *Writer) ensureFileLocked() error {
	if w.file != nil {
		return nil
	}
	return w.openLocked()
}

func (w *Writer) openLocked() error {
	if err := os.MkdirAll(filepath.Dir(w.filename), 0o755); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}
	f, err := os.OpenFile(w.filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, w.options.FileMode)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat log file: %w", err)
	}
	now := time.Now()
	w.file, w.size = f, info.Size()
	if w.options.RotateInterval > 0 {
		w.nextRotate = now.Add(w.options.RotateInterval)
	} else {
		w.nextRotate = time.Time{}
	}
	return nil
}

func (w *Writer) shouldRotateLocked(incoming int64) bool {
	timeDue := !w.nextRotate.IsZero() && !time.Now().Before(w.nextRotate)
	sizeDue := w.options.MaxSize > 0 && w.size > 0 && w.size+incoming > w.options.MaxSize
	return timeDue || sizeDue
}

func (w *Writer) rotateLocked() error {
	file := w.file
	w.file = nil
	if err := file.Close(); err != nil {
		return fmt.Errorf("close log file for rotation: %w", err)
	}
	now := time.Now()
	history, err := w.nextHistoricalFilenameLocked(now)
	if err != nil {
		_ = w.openLocked()
		return err
	}
	if err = os.Rename(w.filename, history); err != nil {
		openErr := w.openLocked()
		if openErr != nil {
			return fmt.Errorf("rotate log file: %w (also reopen current file: %v)", err, openErr)
		}
		return fmt.Errorf("rotate log file: %w", err)
	}
	if err = w.openLocked(); err != nil {
		return fmt.Errorf("open new log file after rotation: %w", err)
	}
	w.rotatedAt = now
	w.enqueueMaintenance()
	return nil
}

func (w *Writer) nextHistoricalFilenameLocked(now time.Time) (string, error) {
	if !w.rotatedAt.IsZero() && now.Format(timestampLayout) != w.rotatedAt.Format(timestampLayout) {
		w.sequence = 0
	}
	for {
		candidate := historicalFilename(w.filename, now, w.sequence)
		_, rawErr := os.Lstat(candidate)
		_, gzipErr := os.Lstat(candidate + ".gz")
		if os.IsNotExist(rawErr) && os.IsNotExist(gzipErr) {
			return candidate, nil
		}
		if rawErr != nil && !os.IsNotExist(rawErr) {
			return "", fmt.Errorf("check historical log name: %w", rawErr)
		}
		if gzipErr != nil && !os.IsNotExist(gzipErr) {
			return "", fmt.Errorf("check compressed historical log name: %w", gzipErr)
		}
		w.sequence++
	}
}

func (w *Writer) needsMaintenance() bool {
	return w.options.MaxBackups > 0 || w.options.MaxAge > 0 ||
		w.options.MaxTotalSize > 0 || w.options.Compress
}

func (w *Writer) enqueueMaintenance() {
	select {
	case w.maintenance <- struct{}{}:
	default:
	}
}

func (w *Writer) runMaintenance() {
	defer close(w.workerDone)
	for range w.maintenance {
		if err := w.maintainSafely(); err != nil {
			w.mu.Lock()
			if w.maintenanceErr == nil {
				w.maintenanceErr = err
			}
			w.mu.Unlock()
		}
	}
}

func (w *Writer) maintainSafely() (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("maintenance worker panic: %v", recovered)
		}
	}()
	return w.maintain()
}

func (w *Writer) maintain() error {
	files, err := scanFiles(w.filename)
	if err != nil {
		return err
	}
	now := time.Now()
	if w.options.MaxAge > 0 {
		kept := files[:0]
		for _, file := range files {
			when := file.timestamp
			if when.IsZero() {
				when = file.modTime
			}
			if now.Sub(when) > w.options.MaxAge {
				if err = os.Remove(file.path); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("remove expired log: %w", err)
				}
				continue
			}
			kept = append(kept, file)
		}
		files = kept
	}
	if w.options.MaxBackups > 0 && len(files) > w.options.MaxBackups {
		for _, file := range files[:len(files)-w.options.MaxBackups] {
			if err = os.Remove(file.path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove excess backup: %w", err)
			}
		}
		files = files[len(files)-w.options.MaxBackups:]
	}
	var compressionErr error
	if w.options.Compress {
		for _, file := range files {
			if !file.compressed {
				if err = compressFile(file.path, w.options.FileMode); err != nil && compressionErr == nil {
					compressionErr = err
				}
			}
		}
		files, err = scanFiles(w.filename)
		if err != nil {
			return errors.Join(compressionErr, err)
		}
	}
	if w.options.MaxTotalSize > 0 {
		currentSize := int64(0)
		if info, err := os.Stat(w.filename); err == nil {
			currentSize = info.Size()
		} else if !os.IsNotExist(err) {
			return errors.Join(compressionErr, fmt.Errorf("stat current log for cleanup: %w", err))
		}
		total := currentSize
		for _, file := range files {
			total += file.size
		}
		for _, file := range files {
			if total <= w.options.MaxTotalSize {
				break
			}
			if err = os.Remove(file.path); err != nil && !os.IsNotExist(err) {
				return errors.Join(compressionErr, fmt.Errorf("remove log for total-size limit: %w", err))
			}
			total -= file.size
		}
	}
	return compressionErr
}
