package rotatefile

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// fileInfo 描述已识别的历史日志文件。
type fileInfo struct {
	path      string
	size      int64
	modTime   time.Time
	timestamp time.Time

	sequence uint64
}

func scanFiles(filename string) ([]fileInfo, error) {
	var entries, err = os.ReadDir(filepath.Dir(filename))
	if err != nil {
		return nil, fmt.Errorf("read log directory: %w", err)
	}
	var files = make([]fileInfo, 0)
	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		var path = filepath.Join(filepath.Dir(filename), entry.Name())
		var ts, sequence, ok = parseHistoricalName(filename, path)
		if !ok {
			continue
		}
		var info os.FileInfo
		info, err = entry.Info()
		if err != nil {
			return nil, fmt.Errorf("stat historical log %q: %w", path, err)
		}
		files = append(files, fileInfo{path: path, size: info.Size(), modTime: info.ModTime(), timestamp: ts, sequence: sequence})
	}
	sortFiles(files)
	return files, nil
}

func sortFiles(files []fileInfo) {
	sort.SliceStable(files, func(i, j int) bool {
		var a, b = files[i], files[j]
		if !a.timestamp.Equal(b.timestamp) {
			return a.timestamp.Before(b.timestamp)
		}
		if a.sequence != b.sequence {
			return a.sequence < b.sequence
		}
		return a.path < b.path
	})
}
