package redact

import (
	"fmt"
	"log/slog"
)

func redactRecord(record slog.Record, redactor Redactor) slog.Record {
	message := redactString(record.Message, redactor)
	if message == record.Message && record.NumAttrs() == 0 {
		return record
	}
	redacted := slog.NewRecord(record.Time, record.Level, message, record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		attr, _ = redactAttr(attr, redactor)
		redacted.AddAttrs(attr)
		return true
	})
	return redacted
}

func redactAttrs(attrs []slog.Attr, redactor Redactor) ([]slog.Attr, bool) {
	for i := range attrs {
		attr, changed := redactAttr(attrs[i], redactor)
		if !changed {
			continue
		}
		redacted := make([]slog.Attr, len(attrs))
		copy(redacted, attrs[:i])
		redacted[i] = attr
		for i++; i < len(attrs); i++ {
			redacted[i], _ = redactAttr(attrs[i], redactor)
		}
		return redacted, true
	}
	return attrs, false
}

func redactAttr(attr slog.Attr, redactor Redactor) (slog.Attr, bool) {
	value, changed := redactValue(attr.Value, redactor)
	if !changed {
		return attr, false
	}
	return slog.Attr{Key: attr.Key, Value: value}, true
}

func redactValue(value slog.Value, redactor Redactor) (slog.Value, bool) {
	value = value.Resolve()
	switch value.Kind() {
	case slog.KindString:
		text := value.String()
		redacted := redactString(text, redactor)
		if redacted == text {
			return value, false
		}
		return slog.StringValue(redacted), true
	case slog.KindGroup:
		attrs := value.Group()
		redacted, changed := redactAttrs(attrs, redactor)
		if !changed {
			return value, false
		}
		return slog.GroupValue(redacted...), true
	case slog.KindAny:
		switch anyValue := value.Any().(type) {
		case error:
			return redactStringValue(value, anyValue.Error(), redactor)
		case fmt.Stringer:
			return redactStringValue(value, anyValue.String(), redactor)
		default:
		}
	default:
	}
	return value, false
}

func redactStringValue(value slog.Value, text string, redactor Redactor) (slog.Value, bool) {
	redacted := redactString(text, redactor)
	if redacted == text {
		return value, false
	}
	return slog.StringValue(redacted), true
}

func redactString(text string, redactor Redactor) string {
	return redactor.Redact(text)
}
