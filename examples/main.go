// Command basic 演示如何将 rotatefile.Writer 与 log/slog 集成使用。
package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/smartwalle/logkit/bufwriter"
	"github.com/smartwalle/logkit/rotatefile"
)

func main() {
	fileWriter, err := rotatefile.New(
		filepath.Join("logs", "app.log"),
		rotatefile.WithMaxSize(1<<10), // 单个当前文件最大 1 KiB，便于演示轮转。
		rotatefile.WithMaxBackups(5),
		rotatefile.WithMaxAge(7*24*time.Hour),
		rotatefile.WithMaxTotalSize(8<<10),
		rotatefile.WithRotateInterval(24*time.Hour),
		rotatefile.WithCompression(false),
	)

	if err != nil {
		slog.Error("create log writer", "error", err)
		os.Exit(1)
	}
	writer, err := bufwriter.New(
		fileWriter,
		bufwriter.WithBufferSize(512), // 使用较小缓冲区，便于演示缓冲与轮转的组合。
		bufwriter.WithFlushInterval(time.Second),
	)
	if err != nil {
		if closeErr := fileWriter.Close(); closeErr != nil {
			slog.Error("close log writer", "error", closeErr)
		}
		slog.Error("create buffered writer", "error", err)
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

	for i := 1; i <= 100; i++ {
		logger.Info("HTTP request completed",
			"request_id", fmt.Sprintf("req-%04d", i),
			"method", "GET",
			"path", "/api/v1/orders",
			"status", 200,
			"duration", 42*time.Millisecond,
		)
	}
}
