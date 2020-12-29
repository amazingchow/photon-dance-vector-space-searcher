package storage

import (
	"github.com/amazingchow/photon-dance-vector-space-searcher/internal/common"
)

// Persister 持久化接口定义
type Persister interface {
	Init() (err error)
	Destroy() (err error)
	Writable(file *common.File) (path string, err error)
	Put(file *common.File) (string, error)
	Readable(file *common.File) (path string, err error)
	Get(file *common.File) (string, error)
	Abort(file *common.File) error
	Delete(file *common.File) error
}
