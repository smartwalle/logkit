package logkit

import (
	"context"
	"log/slog"
)

// EnrichHandler 在将记录交给下游 EnrichHandler 前附加从 Context 提取的属性。
type EnrichHandler struct {
	next    slog.Handler
	extract func(context.Context) []slog.Attr
}

// NewEnrichHandler 创建一个 EnrichHandler。extract 返回的属性会追加到日志记录顶层。
func NewEnrichHandler(next slog.Handler, extract func(context.Context) []slog.Attr) *EnrichHandler {
	return &EnrichHandler{next: next, extract: extract}
}

// Enabled 报告下游 EnrichHandler 是否启用了 level。
func (h *EnrichHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

// Handle 从 Context 提取属性并附加到记录后交给下游 EnrichHandler。
func (h *EnrichHandler) Handle(ctx context.Context, record slog.Record) error {
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

// WithAttrs 返回附加 attrs 后的 EnrichHandler。
func (h *EnrichHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &EnrichHandler{
		next:    h.next.WithAttrs(attrs),
		extract: h.extract,
	}
}

// WithGroup 返回在下游 EnrichHandler 中进入 name 分组后的 EnrichHandler。
func (h *EnrichHandler) WithGroup(name string) slog.Handler {
	return &EnrichHandler{
		next:    h.next.WithGroup(name),
		extract: h.extract,
	}
}
