// Command basic 演示如何将 rotate.Writer 与 log/slog 集成使用。
package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/smartwalle/logkit/rotate"
)

func main() {
	writer, err := rotate.NewWriter(
		filepath.Join("logs", "app.log"),
		rotate.WithMaxSize(1<<10), // 单个当前文件最大 1 KiB，便于演示轮转。
		rotate.WithMaxBackups(5),
		rotate.WithMaxAge(7*24*time.Hour),
		rotate.WithMaxTotalSize(8<<10),
		rotate.WithRotateInterval(24*time.Hour),
		rotate.WithCompression(false),
	)
	if err != nil {
		slog.Error("create log writer", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			slog.Error("close log writer", "error", err)
		}
	}()

	logger := slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	for i := 0; i < 100; i++ {
		logger.Info("application started", "version", "v1.0.0")
		logger.Info("request completed", "request_id", "req-123", "duration", 42*time.Millisecond)
		logger.Warn("cache miss", "key", "profile:42")
	}
}
