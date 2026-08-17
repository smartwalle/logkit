// Package redact 提供基于 slog 的内容级日志脱敏 Handler。
package redact

import (
	"context"
	"log/slog"
)

// Handler 在将记录交给下游 Handler 前对字符串内容执行脱敏。
type Handler struct {
	next     slog.Handler
	redactor Redactor
}

// NewHandler 创建一个对日志字符串内容进行脱敏的 Handler。
func NewHandler(next slog.Handler, redactor Redactor) *Handler {
	if redactor == nil {
		panic("redactor must not be nil")
	}
	return &Handler{next: next, redactor: redactor}
}

// Enabled 报告下游 Handler 是否启用了 level。
func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

// Handle 对记录中的字符串内容执行脱敏后交给下游 Handler。
func (h *Handler) Handle(ctx context.Context, record slog.Record) error {
	if !h.next.Enabled(ctx, record.Level) {
		return nil
	}
	return h.next.Handle(ctx, redactRecord(record, h.redactor))
}

// WithAttrs 返回附加 attrs 后的 Handler。
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	attrs, _ = redactAttrs(attrs, h.redactor)
	return &Handler{
		next:     h.next.WithAttrs(attrs),
		redactor: h.redactor,
	}
}

// WithGroup 返回在下游 Handler 中进入 name 分组后的 Handler。
func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{
		next:     h.next.WithGroup(name),
		redactor: h.redactor,
	}
}
