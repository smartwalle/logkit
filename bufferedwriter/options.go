package bufferedwriter

import (
	"fmt"
	"time"
)

const (
	defaultBufferSize    = 32 << 10
	defaultFlushInterval = 5 * time.Second
)

// Option 用于配置 Writer。
type Option func(*options)

type options struct {
	bufferSize    int
	flushInterval time.Duration
}

// WithBufferSize 设置内存缓冲区大小，单位为字节。
func WithBufferSize(size int) Option { return func(o *options) { o.bufferSize = size } }

// WithFlushInterval 设置定时刷新间隔。值为 0 时关闭定时刷新。
func WithFlushInterval(interval time.Duration) Option {
	return func(o *options) { o.flushInterval = interval }
}

func resolveOptions(opts []Option) (options, error) {
	o := options{bufferSize: defaultBufferSize, flushInterval: defaultFlushInterval}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.bufferSize <= 0 {
		return o, fmt.Errorf("BufferSize must be positive")
	}
	if o.flushInterval < 0 {
		return o, fmt.Errorf("FlushInterval must not be negative")
	}
	return o, nil
}
