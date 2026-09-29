// Command basic 演示如何将 rotatefile.Writer 与 log/slog 集成使用。
package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/smartwalle/logkit"
	"github.com/smartwalle/logkit/rotatefile"
)

func main() {
	file, err := rotatefile.New(
		filepath.Join("logs", "app.log"),
		rotatefile.WithMaxSize(1024*10),
		rotatefile.WithMaxBackups(20),
		rotatefile.WithMaxAge(7*24*time.Hour),
		rotatefile.WithMaxTotalSize(1024*100),
		rotatefile.WithRotateInterval(24*time.Hour),
		rotatefile.WithCompression(false),
	)
	if err != nil {
		slog.Error("create log writer", "error", err)
		os.Exit(1)
	}
	defer file.Close()

	buffer, err := logkit.NewBuffer(
		file,
		512,
		time.Second,
	)
	if err != nil {
		slog.Error("create buffered writer", "error", err)
		os.Exit(1)
	}
	defer buffer.Close()

	logger := slog.New(slog.NewJSONHandler(buffer, &slog.HandlerOptions{
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
