package logkit

import (
	"context"
	"fmt"
	"log/slog"
)

// Redactor 对字符串内容执行脱敏转换。实现必须可被并发调用。
type Redactor interface {
	Redact(string) string
}

// RedactHandler 在将记录交给下游 RedactHandler 前对字符串内容执行脱敏。
type RedactHandler struct {
	next     slog.Handler
	redactor Redactor
}

// NewRedactHandler 创建一个对日志字符串内容进行脱敏的 RedactHandler。
func NewRedactHandler(next slog.Handler, redactor Redactor) *RedactHandler {
	if redactor == nil {
		panic("redactor must not be nil")
	}
	return &RedactHandler{next: next, redactor: redactor}
}

// Enabled 报告下游 RedactHandler 是否启用了 level。
func (h *RedactHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

// Handle 对记录中的字符串内容执行脱敏后交给下游 RedactHandler。
func (h *RedactHandler) Handle(ctx context.Context, record slog.Record) error {
	return h.next.Handle(ctx, h.redactRecord(record))
}

// WithAttrs 返回附加 attrs 后的 RedactHandler。
func (h *RedactHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	attrs, _ = h.redactAttrs(attrs)
	return &RedactHandler{
		next:     h.next.WithAttrs(attrs),
		redactor: h.redactor,
	}
}

// WithGroup 返回在下游 RedactHandler 中进入 name 分组后的 RedactHandler。
func (h *RedactHandler) WithGroup(name string) slog.Handler {
	return &RedactHandler{
		next:     h.next.WithGroup(name),
		redactor: h.redactor,
	}
}

func (h *RedactHandler) redactRecord(record slog.Record) slog.Record {
	message := h.redactString(record.Message)
	if message == record.Message && record.NumAttrs() == 0 {
		return record
	}
	redacted := slog.NewRecord(record.Time, record.Level, message, record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		attr, _ = h.redactAttr(attr)
		redacted.AddAttrs(attr)
		return true
	})
	return redacted
}

func (h *RedactHandler) redactAttrs(attrs []slog.Attr) ([]slog.Attr, bool) {
	for i := range attrs {
		attr, changed := h.redactAttr(attrs[i])
		if !changed {
			continue
		}
		redacted := make([]slog.Attr, len(attrs))
		copy(redacted, attrs[:i])
		redacted[i] = attr
		for i++; i < len(attrs); i++ {
			redacted[i], _ = h.redactAttr(attrs[i])
		}
		return redacted, true
	}
	return attrs, false
}

func (h *RedactHandler) redactAttr(attr slog.Attr) (slog.Attr, bool) {
	value, changed := h.redactValue(attr.Value)
	if !changed {
		return attr, false
	}
	return slog.Attr{Key: attr.Key, Value: value}, true
}

func (h *RedactHandler) redactValue(value slog.Value) (slog.Value, bool) {
	value = value.Resolve()
	switch value.Kind() {
	case slog.KindString:
		text := value.String()
		redacted := h.redactString(text)
		if redacted == text {
			return value, false
		}
		return slog.StringValue(redacted), true
	case slog.KindGroup:
		attrs := value.Group()
		redacted, changed := h.redactAttrs(attrs)
		if !changed {
			return value, false
		}
		return slog.GroupValue(redacted...), true
	case slog.KindAny:
		switch anyValue := value.Any().(type) {
		case fmt.Stringer:
			return h.redactStringValue(value, anyValue.String())
		default:
		}
	default:
	}
	return value, false
}

func (h *RedactHandler) redactStringValue(value slog.Value, text string) (slog.Value, bool) {
	var redacted = h.redactString(text)
	if redacted == text {
		return value, false
	}
	return slog.StringValue(redacted), true
}

func (h *RedactHandler) redactString(text string) string {
	return h.redactor.Redact(text)
}
