// Package rotatefile 提供并发安全的日志文件轮转 File。
package rotatefile

import (
	"fmt"
	"io"
	"log"
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

	maintenance chan struct{}
	workerDone  chan struct{}
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
	file := &File{
		filename:    filename,
		options:     nOpts,
		maintenance: make(chan struct{}, 1),
		workerDone:  make(chan struct{}),
	}
	if err = file.openLocked(); err != nil {
		return nil, err
	}
	go file.runMaintenance()
	if file.needsMaintenance() {
		file.enqueueMaintenance()
	}
	return file, nil
}

// Write 将 p 写入当前日志文件，并与其他 Write 调用串行执行。
func (f *File) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, io.ErrClosedPipe
	}
	if err := f.ensureFileLocked(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if f.shouldRotateLocked(int64(len(p))) {
		if err := f.rotateLocked(); err != nil {
			return 0, err
		}
	}
	n, err := f.file.Write(p)
	f.size += int64(n)
	if err != nil {
		return n, err
	}
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

// Close 停止维护任务、等待已排队的压缩和清理完成，并关闭日志文件。
func (f *File) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	close(f.maintenance)
	f.mu.Unlock()
	<-f.workerDone

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return nil
	}

	var file = f.file
	f.file = nil

	return file.Close()
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
		_ = file.Close()
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
	var file = f.file
	f.file = nil

	if err := file.Close(); err != nil {
		return fmt.Errorf("close log file for rotation: %w", err)
	}
	var now = time.Now()
	var history, err = f.nextHistoricalFilenameLocked(now)
	if err != nil {
		_ = f.openLocked()
		return err
	}
	if err = os.Rename(f.filename, history); err != nil {
		_ = f.openLocked()
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
		_, err := os.Lstat(candidate)
		if os.IsNotExist(err) {
			return candidate, nil
		}
		if err != nil {
			return "", fmt.Errorf("check historical log name: %w", err)
		}
		f.sequence++
	}
}

func (f *File) needsMaintenance() bool {
	return f.options.MaxBackups > 0 || f.options.MaxAge > 0 || f.options.MaxTotalSize > 0
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
			log.Printf("rotatefile: maintenance failed: %v", err)
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
	var files, err = scanFiles(f.filename)
	if err != nil {
		return err
	}
	var now = time.Now()
	if f.options.MaxAge > 0 {
		var kept = files[:0]
		for _, file := range files {
			var when = file.timestamp
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

	if f.options.MaxTotalSize > 0 {
		var info os.FileInfo
		if info, err = os.Stat(f.filename); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("stat current log for cleanup: %w", err)
		}

		var currentSize = int64(0)
		if info != nil {
			currentSize = info.Size()
		}

		var total = currentSize
		for _, file := range files {
			total += file.size
		}
		for _, file := range files {
			if total <= f.options.MaxTotalSize {
				break
			}
			if err = os.Remove(file.path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove log for total-size limit: %w", err)
			}
			total -= file.size
		}
	}
	return nil
}
