package storage

import (
	"context"

	"github.com/amazingchow/photon-dance-vector-space-searcher/internal/common"
)

// Persister 持久化接口定义
type Persister interface {
	Init() (err error)
	Destroy() (err error)
	Writable(ctx context.Context, file *common.File) (path string, err error)
	Put(ctx context.Context, file *common.File) (string, error)
	Readable(ctx context.Context, file *common.File) (path string, err error)
	Get(ctx context.Context, file *common.File) (string, error)
	Abort(ctx context.Context, file *common.File) error
	Delete(ctx context.Context, file *common.File) error
}
