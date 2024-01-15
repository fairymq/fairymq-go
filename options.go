package fairymqgo

import "time"

type options struct {
	attempts    uint
	readTimeout time.Duration
}

type Option func(*options)

func WithAttempts(attempts uint) Option {
	return func(o *options) {
		o.attempts = attempts
	}
}

func WithReadTimeout(t time.Duration) Option {
	return func(o *options) {
		o.readTimeout = t
	}
}
