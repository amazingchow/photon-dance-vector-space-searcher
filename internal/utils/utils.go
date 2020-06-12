package utils

import (
	"os"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rs/zerolog/log"
)

// FileSize 计算文件大小.
func FileSize(file string) int64 {
	fd, err := os.Stat(file)
	if err != nil {
		log.Error().Err(err).Msgf("cannot stat file, file=%s", file)
		return 0
	}
	return fd.Size()
}

// BackoffPolicy 自定义backoff策略.
func BackoffPolicy() backoff.BackOff {
	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = 600 * time.Millisecond
	policy.Multiplier = 10.0
	policy.MaxInterval = 30 * time.Second
	policy.MaxElapsedTime = 90 * time.Second

	repeater := backoff.WithMaxRetries(policy, 5)
	repeater.Reset()
	return repeater
}
