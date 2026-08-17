// Package enrich 提供基于 slog 的 Context 属性增强 Handler。
package enrich

import (
	"context"
	"log/slog"
)

// Handler 在将记录交给下游 Handler 前附加从 Context 提取的属性。
type Handler struct {
	next    slog.Handler
	extract func(context.Context) []slog.Attr
}

// NewHandler 创建一个 Handler。extract 返回的属性会追加到日志记录顶层。
func NewHandler(next slog.Handler, extract func(context.Context) []slog.Attr) *Handler {
	return &Handler{next: next, extract: extract}
}

// Enabled 报告下游 Handler 是否启用了 level。
func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

// Handle 从 Context 提取属性并附加到记录后交给下游 Handler。
func (h *Handler) Handle(ctx context.Context, record slog.Record) error {
	if !h.next.Enabled(ctx, record.Level) {
		return nil
	}
	attrs := h.extract(ctx)
	if len(attrs) == 0 {
		return h.next.Handle(ctx, record)
	}
	enriched := record.Clone()
	enriched.AddAttrs(attrs...)
	return h.next.Handle(ctx, enriched)
}

// WithAttrs 返回附加 attrs 后的 Handler。
func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &Handler{
		next:    h.next.WithAttrs(attrs),
		extract: h.extract,
	}
}

// WithGroup 返回在下游 Handler 中进入 name 分组后的 Handler。
func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{
		next:    h.next.WithGroup(name),
		extract: h.extract,
	}
}
