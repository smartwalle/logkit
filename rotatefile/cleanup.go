package rotatefile

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// fileInfo 描述已识别的历史日志文件。
type fileInfo struct {
	path       string
	size       int64
	modTime    time.Time
	timestamp  time.Time
	compressed bool

	sequence uint64
}

func scanFiles(filename string) ([]fileInfo, error) {
	entries, err := os.ReadDir(filepath.Dir(filename))
	if err != nil {
		return nil, fmt.Errorf("read log directory: %w", err)
	}
	files := make([]fileInfo, 0)
	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		path := filepath.Join(filepath.Dir(filename), entry.Name())
		ts, sequence, compressed, ok := parseHistoricalName(filename, path)
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("stat historical log %q: %w", path, err)
		}
		files = append(files, fileInfo{path: path, size: info.Size(), modTime: info.ModTime(), timestamp: ts, compressed: compressed, sequence: sequence})
	}
	sortFiles(files)
	return files, nil
}

func sortFiles(files []fileInfo) {
	sort.SliceStable(files, func(i, j int) bool {
		a, b := files[i], files[j]
		if !a.timestamp.Equal(b.timestamp) {
			return a.timestamp.Before(b.timestamp)
		}
		if a.sequence != b.sequence {
			return a.sequence < b.sequence
		}
		return a.path < b.path
	})
}

func removeTemporaryFiles(filename string) error {
	entries, err := os.ReadDir(filepath.Dir(filename))
	if err != nil {
		return fmt.Errorf("read log directory: %w", err)
	}
	base := filepath.Base(filename) + "."
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), base) || !strings.HasSuffix(entry.Name(), ".gz.tmp") {
			continue
		}
		rawPath := filepath.Join(filepath.Dir(filename), strings.TrimSuffix(entry.Name(), ".gz.tmp"))
		if _, _, _, ok := parseHistoricalName(filename, rawPath); !ok {
			continue
		}
		if err = os.Remove(filepath.Join(filepath.Dir(filename), entry.Name())); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale compression file: %w", err)
		}
	}
	return nil
}

func compressFile(path string, mode os.FileMode) error {
	src, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open log for compression: %w", err)
	}
	defer src.Close()

	tmp := path + ".gz.tmp"
	dst, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return fmt.Errorf("create compressed log: %w", err)
	}
	gz := gzip.NewWriter(dst)
	_, copyErr := io.Copy(gz, src)
	closeGzipErr := gz.Close()
	closeFileErr := dst.Close()
	if copyErr != nil || closeGzipErr != nil || closeFileErr != nil {
		_ = os.Remove(tmp)
		if copyErr != nil {
			return fmt.Errorf("compress log: %w", copyErr)
		}
		if closeGzipErr != nil {
			return fmt.Errorf("finish compressed log: %w", closeGzipErr)
		}
		return fmt.Errorf("close compressed log: %w", closeFileErr)
	}
	if err = os.Rename(tmp, path+".gz"); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("publish compressed log: %w", err)
	}
	if err = os.Remove(path); err != nil {
		return fmt.Errorf("remove uncompressed log after compression: %w", err)
	}
	return nil
}
