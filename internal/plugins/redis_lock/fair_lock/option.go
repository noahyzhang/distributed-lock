package fair_lock

type FairLockOption func(fairLock *RedisFairLock)

func WithGoroutineWaitTimeMs(goroutineWaitTimeMs int64) FairLockOption {
	return func(r *RedisFairLock) {
		r.goroutineWaitTimeMs = goroutineWaitTimeMs
	}
}
