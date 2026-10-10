// Package rotatefile 提供并发安全的日志文件轮转 File。
package rotatefile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// File 是一个向文件追加日志并按大小或时间自动轮转的 io.Writer。
//
// File 可被多个 goroutine 并发调用；它独占管理指定的日志路径，不与外部
// logrotate 混用。
type File struct {
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

// New 创建 filename 的父目录（如有必要）并以追加模式打开日志文件。
func New(filename string, opts ...Option) (*File, error) {
	if !validFilename(filename) {
		return nil, fmt.Errorf("filename must not be empty")
	}
	var err error
	nOpts, err := resolveOptions(opts)
	if err != nil {
		return nil, err
	}
	w := &File{
		filename:    filename,
		options:     nOpts,
		maintenance: make(chan struct{}, 1),
		workerDone:  make(chan struct{}),
		closeDone:   make(chan struct{}),
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
func (f *File) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, io.ErrClosedPipe
	}
	if err := f.ensureFileLocked(); err != nil {
		f.recordRuntimeErrorLocked(err)
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	var rotationErr error
	if f.shouldRotateLocked(int64(len(p))) {
		if err := f.rotateLocked(); err != nil {
			// 轮转失败时，优先继续使用已恢复打开的当前日志文件。
			if f.file == nil {
				f.recordRuntimeErrorLocked(err)
				return 0, err
			}
			rotationErr = err
		}
	}
	n, err := f.file.Write(p)
	f.size += int64(n)
	if err != nil {
		writeErr := fmt.Errorf("write log file: %w", err)
		result := errors.Join(rotationErr, writeErr)
		f.recordRuntimeErrorLocked(result)
		return n, result
	}
	if n != len(p) {
		writeErr := errors.New("write log file: short write")
		result := errors.Join(rotationErr, writeErr)
		f.recordRuntimeErrorLocked(result)
		return n, result
	}
	if rotationErr != nil {
		f.recordRuntimeErrorLocked(rotationErr)
		return n, rotationErr
	}
	return n, nil
}

// Close 停止维护任务、等待已排队的压缩和清理完成，并关闭日志文件。
// Close 可重复调用；每次调用均返回相同的最终错误。
func (f *File) Close() error {
	f.mu.Lock()
	if f.closed {
		done := f.closeDone
		f.mu.Unlock()
		<-done
		f.mu.Lock()
		err := f.closeErr
		f.mu.Unlock()
		return err
	}
	f.closed = true
	close(f.maintenance)
	f.mu.Unlock()
	<-f.workerDone

	f.mu.Lock()
	var closeErr error
	if f.file != nil {
		closeErr = f.file.Close()
		f.file = nil
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close log file: %w", closeErr)
	}
	f.closeErr = errors.Join(f.runtimeErr, f.maintenanceErr, closeErr)
	result := f.closeErr
	close(f.closeDone)
	f.mu.Unlock()
	return result
}

func (f *File) recordRuntimeErrorLocked(err error) {
	if err != nil && f.runtimeErr == nil {
		f.runtimeErr = err
	}
}

func (f *File) ensureFileLocked() error {
	if f.file != nil {
		return nil
	}
	return f.openLocked()
}

func (f *File) openLocked() error {
	if err := os.MkdirAll(filepath.Dir(f.filename), 0o755); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}
	file, err := os.OpenFile(f.filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, f.options.FileMode)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("stat log file: %w", err)
	}
	now := time.Now()
	f.file, f.size = file, info.Size()
	if f.options.RotateInterval > 0 {
		f.nextRotate = now.Add(f.options.RotateInterval)
	} else {
		f.nextRotate = time.Time{}
	}
	return nil
}

func (f *File) shouldRotateLocked(incoming int64) bool {
	timeDue := !f.nextRotate.IsZero() && !time.Now().Before(f.nextRotate)
	sizeDue := f.options.MaxSize > 0 && f.size > 0 && f.size+incoming > f.options.MaxSize
	return timeDue || sizeDue
}

func (f *File) rotateLocked() error {
	file := f.file
	f.file = nil
	if err := file.Close(); err != nil {
		return fmt.Errorf("close log file for rotation: %w", err)
	}
	now := time.Now()
	history, err := f.nextHistoricalFilenameLocked(now)
	if err != nil {
		_ = f.openLocked()
		return err
	}
	if err = os.Rename(f.filename, history); err != nil {
		openErr := f.openLocked()
		if openErr != nil {
			return fmt.Errorf("rotate log file: %w (also reopen current file: %v)", err, openErr)
		}
		return fmt.Errorf("rotate log file: %w", err)
	}
	if err = f.openLocked(); err != nil {
		return fmt.Errorf("open new log file after rotation: %w", err)
	}
	f.rotatedAt = now
	f.enqueueMaintenance()
	return nil
}

func (f *File) nextHistoricalFilenameLocked(now time.Time) (string, error) {
	if !f.rotatedAt.IsZero() && now.Format(timestampLayout) != f.rotatedAt.Format(timestampLayout) {
		f.sequence = 0
	}
	for {
		candidate := historicalFilename(f.filename, now, f.sequence)
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
		f.sequence++
	}
}

func (f *File) needsMaintenance() bool {
	return f.options.MaxBackups > 0 || f.options.MaxAge > 0 ||
		f.options.MaxTotalSize > 0 || f.options.Compress
}

func (f *File) enqueueMaintenance() {
	select {
	case f.maintenance <- struct{}{}:
	default:
	}
}

func (f *File) runMaintenance() {
	defer close(f.workerDone)
	for range f.maintenance {
		if err := f.maintainSafely(); err != nil {
			f.mu.Lock()
			if f.maintenanceErr == nil {
				f.maintenanceErr = err
			}
			f.mu.Unlock()
		}
	}
}

func (f *File) maintainSafely() (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("maintenance worker panic: %v", recovered)
		}
	}()
	return f.maintain()
}

func (f *File) maintain() error {
	files, err := scanFiles(f.filename)
	if err != nil {
		return err
	}
	now := time.Now()
	if f.options.MaxAge > 0 {
		kept := files[:0]
		for _, file := range files {
			when := file.timestamp
			if when.IsZero() {
				when = file.modTime
			}
			if now.Sub(when) > f.options.MaxAge {
				if err = os.Remove(file.path); err != nil && !os.IsNotExist(err) {
					return fmt.Errorf("remove expired log: %w", err)
				}
				continue
			}
			kept = append(kept, file)
		}
		files = kept
	}
	if f.options.MaxBackups > 0 && len(files) > f.options.MaxBackups {
		for _, file := range files[:len(files)-f.options.MaxBackups] {
			if err = os.Remove(file.path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove excess backup: %w", err)
			}
		}
		files = files[len(files)-f.options.MaxBackups:]
	}
	var compressionErr error
	if f.options.Compress {
		for _, file := range files {
			if !file.compressed {
				if err = compressFile(file.path, f.options.FileMode); err != nil && compressionErr == nil {
					compressionErr = err
				}
			}
		}
		files, err = scanFiles(f.filename)
		if err != nil {
			return errors.Join(compressionErr, err)
		}
	}
	if f.options.MaxTotalSize > 0 {
		currentSize := int64(0)
		if info, err := os.Stat(f.filename); err == nil {
			currentSize = info.Size()
		} else if !os.IsNotExist(err) {
			return errors.Join(compressionErr, fmt.Errorf("stat current log for cleanup: %w", err))
		}
		total := currentSize
		for _, file := range files {
			total += file.size
		}
		for _, file := range files {
			if total <= f.options.MaxTotalSize {
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
