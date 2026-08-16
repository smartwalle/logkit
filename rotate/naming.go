package rotate

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const timestampLayout = "20060102-150405"

func historicalFilename(current string, t time.Time, sequence uint64) string {
	dir := filepath.Dir(current)
	base := filepath.Base(current)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	return filepath.Join(dir, name+"."+t.Format(timestampLayout)+"-"+leftPadSequence(sequence)+ext)
}

func leftPadSequence(sequence uint64) string {
	return fmt.Sprintf("%04d", sequence)
}

// parseHistoricalName 仅识别当前 Writer 生成的历史文件，避免将具有相同前缀的
// 无关文件纳入清理范围。
func parseHistoricalName(current, path string) (timestamp time.Time, sequence uint64, compressed bool, ok bool) {
	if filepath.Dir(current) != filepath.Dir(path) {
		return time.Time{}, 0, false, false
	}
	base := filepath.Base(current)
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)
	name := filepath.Base(path)
	if strings.HasSuffix(name, ".gz") {
		compressed = true
		name = strings.TrimSuffix(name, ".gz")
	}
	if ext != "" {
		if !strings.HasSuffix(name, ext) {
			return time.Time{}, 0, false, false
		}
		name = strings.TrimSuffix(name, ext)
	}
	prefix := stem + "."
	if !strings.HasPrefix(name, prefix) {
		return time.Time{}, 0, false, false
	}
	rest := strings.TrimPrefix(name, prefix)
	if len(rest) <= len(timestampLayout)+1 || rest[len(timestampLayout)] != '-' {
		return time.Time{}, 0, false, false
	}
	timestamp, err := time.ParseInLocation(timestampLayout, rest[:len(timestampLayout)], time.Local)
	if err != nil {
		return time.Time{}, 0, false, false
	}
	sequenceText := rest[len(timestampLayout)+1:]
	if len(sequenceText) < 4 {
		return time.Time{}, 0, false, false
	}
	sequence, err = strconv.ParseUint(sequenceText, 10, 64)
	if err != nil {
		return time.Time{}, 0, false, false
	}
	return timestamp, sequence, compressed, true
}
