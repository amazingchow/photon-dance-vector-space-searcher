package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rs/zerolog/log"
)

var (
	// ErrServiceUnavailable 服务不可用错误
	ErrServiceUnavailable = fmt.Errorf("service unavailable")
	// ErrContextDone context超时错误
	ErrContextDone = fmt.Errorf("context done")
)

// IsContextDone 检查context是否超时.
func IsContextDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// FileSize 计算文件大小.
func FileSize(fn string) int64 {
	fd, err := os.Stat(fn)
	if err != nil {
		log.Error().Err(err).Msgf("cannot stat file, file=%s", fn)
		return 0
	}
	return fd.Size()
}

// FileExist 检查文件/文件夹是否存在.
func FileExist(fn string) bool {
	if _, err := os.Stat(fn); err != nil {
		if os.IsNotExist(err) {
			return false
		}
		log.Fatal().Err(err).Msgf("cannot stat file, file=%s", fn)
	}
	return true
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

func loadConfig(cfgPath string, ptr interface{}) error {
	if ptr == nil {
		return fmt.Errorf("ptr of type (%T) is nil", ptr)
	}

	data, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		return fmt.Errorf("open file (%s) with err (%v)", cfgPath, err)
	}

	if err := json.Unmarshal(data, ptr); err != nil {
		return fmt.Errorf("json unmarshal file (%s) with err (%v)", cfgPath, err)
	}

	return nil
}

// LoadConfigOrPanic 加载配置, 如果加载失败就直接panic.
func LoadConfigOrPanic(cfgPath string, ptr interface{}) {
	if err := loadConfig(cfgPath, ptr); err != nil {
		log.Fatal().Err(err)
	}
}
