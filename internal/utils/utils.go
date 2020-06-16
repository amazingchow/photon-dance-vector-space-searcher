package utils

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
