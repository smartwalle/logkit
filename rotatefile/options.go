package rotatefile

import (
	"fmt"
	"io/fs"
	"strings"
	"time"
)

// Option 用于配置 Writer。
type Option func(*options)

type options struct {
	MaxSize        int64
	MaxBackups     int
	MaxAge         time.Duration
	MaxTotalSize   int64
	RotateInterval time.Duration
	Compress       bool
	FileMode       fs.FileMode
}

// WithMaxSize 设置单个当前日志文件的最大字节数。
// 值为 0 时关闭按大小轮转。
func WithMaxSize(size int64) Option { return func(o *options) { o.MaxSize = size } }

// WithMaxBackups 设置保留的历史日志文件数量。
// 值为 0 时不限制历史文件数量。
func WithMaxBackups(count int) Option { return func(o *options) { o.MaxBackups = count } }

// WithMaxAge 设置历史日志的最长保留时间。
// 值为 0 时关闭按时间清理。
func WithMaxAge(age time.Duration) Option { return func(o *options) { o.MaxAge = age } }

// WithMaxTotalSize 设置异步清理时当前文件和历史文件占用空间的总上限。
// 它不是实时磁盘配额；当前文件本身超过该值时不会被删除。值为 0 时关闭按总大小清理。
func WithMaxTotalSize(size int64) Option { return func(o *options) { o.MaxTotalSize = size } }

// WithRotateInterval 设置时间轮转间隔。
// 达到间隔后的下一次写入会触发轮转；值为 0 时关闭按时间轮转。
func WithRotateInterval(interval time.Duration) Option {
	return func(o *options) { o.RotateInterval = interval }
}

// WithCompression 设置是否异步 gzip 压缩历史日志。
func WithCompression(enabled bool) Option { return func(o *options) { o.Compress = enabled } }

// WithFileMode 设置创建日志文件和压缩文件时使用的权限。
// 权限必须包含 owner 写位；启用压缩时还必须包含 owner 读位。
func WithFileMode(mode fs.FileMode) Option { return func(o *options) { o.FileMode = mode } }

func resolveOptions(opts []Option) (options, error) {
	o := options{FileMode: 0o644}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o.normalize()
}

func (o options) normalize() (options, error) {
	if o.MaxSize < 0 {
		return o, fmt.Errorf("MaxSize must not be negative")
	}
	if o.MaxBackups < 0 {
		return o, fmt.Errorf("MaxBackups must not be negative")
	}
	if o.MaxAge < 0 {
		return o, fmt.Errorf("MaxAge must not be negative")
	}
	if o.MaxTotalSize < 0 {
		return o, fmt.Errorf("MaxTotalSize must not be negative")
	}
	if o.RotateInterval < 0 {
		return o, fmt.Errorf("RotateInterval must not be negative")
	}
	if o.FileMode.Perm()&0o200 == 0 {
		return o, fmt.Errorf("FileMode must include an owner write permission")
	}
	if o.Compress && o.FileMode.Perm()&0o400 == 0 {
		return o, fmt.Errorf("FileMode must include an owner read permission when compression is enabled")
	}
	return o, nil
}

func validFilename(filename string) bool { return strings.TrimSpace(filename) != "" }
