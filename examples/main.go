// Command basic 演示如何将 rotatefile.File 与 log/slog 集成使用。
package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/smartwalle/logkit/asyncwriter"
	"github.com/smartwalle/logkit/bufferwriter"
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
	)
	if err != nil {
		slog.Error("create log writer", "error", err)
		os.Exit(1)
	}
	defer file.Close()

	bufWriter, err := bufferwriter.New(
		file,
		512,
		time.Second,
	)
	if err != nil {
		slog.Error("create buffered writer", "error", err)
		os.Exit(1)
	}
	defer bufWriter.Close()

	asyncWriter := asyncwriter.New(bufWriter, 10)
	defer asyncWriter.Close()

	logger := slog.New(slog.NewJSONHandler(asyncWriter, &slog.HandlerOptions{
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
