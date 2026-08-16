// Package logkit 提供可组合的日志基础设施。
package logkit

import "errors"

// ErrWriterClosed 表示 Writer 已关闭，不能再执行写入操作。
var ErrWriterClosed = errors.New("writer is closed")
