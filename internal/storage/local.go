package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"

	"github.com/amazingchow/photon-dance-vector-space-searcher/internal/common"
)

// LocalStorage 提供本地持久化服务
type LocalStorage struct {
	root string
}

// NewLocalStorage 返回本地持久化服务实例.
// For Unix system, path should meet unix naming rules, like "/path/to/a/b/c".
func NewLocalStorage(path string) *LocalStorage {
	return &LocalStorage{
		root: path,
	}
}

// Init 初始化用于本地持久化服务的资源.
func (p *LocalStorage) Init() error {
	log.Info().Msg("load local-storage plugin")
	return os.MkdirAll(p.root, 0755)
}

// Destroy 清除本地持久化服务的资源.
func (p *LocalStorage) Destroy() error {
	log.Info().Msg("unload local-storage plugin")
	return nil
}

// LocalPath 本地文件持久化路径.
func (p *LocalStorage) LocalPath(file *common.File) string {
	return filepath.Join(p.root, fmt.Sprintf("%s/%s.%s",
		common.FileType2FileTypeName[file.Type], file.Name, common.FileType2FileSuffix[file.Type]))
}

// Writable 检查当前文件是否可写, 可以就返回待写入的持久化路径.
func (p *LocalStorage) Writable(file *common.File) (string, error) {
	return p.LocalPath(file), nil
}

// Put 将当前文件写入本地磁盘.
func (p *LocalStorage) Put(file *common.File) (string, error) {
	path := p.LocalPath(file)

	fw, err := os.Create(path)
	if err != nil {
		log.Error().Err(err).Msgf("cannot write file, file=%s", path)
		return "", err
	}
	defer fw.Close()

	w := bufio.NewWriter(fw)
	defer w.Flush()

	for idx, line := range file.Body {
		if idx == len(file.Body)-1 {
			if _, err = w.WriteString(line); err != nil {
				log.Error().Err(err).Msgf("cannot write file, file=%s", path)
				return "", err
			}
		} else {
			if _, err = w.WriteString(line + "\n"); err != nil {
				log.Error().Err(err).Msgf("cannot write file, file=%s", path)
				return "", err
			}
		}
	}

	log.Debug().Msgf("write file successfully, file=%s", path)

	return path, nil
}

// Readable 检查当前文件是否可读, 可以就返回待读取的持久化路径.
func (p *LocalStorage) Readable(file *common.File) (string, error) {
	path := p.LocalPath(file)

	if _, err := os.Stat(path); err != nil {
		log.Error().Err(err).Msgf("cannot stat file, file=%s", path)
		return "", err
	}

	return path, nil
}

// Get 从本地磁盘上读取当前文件.
func (p *LocalStorage) Get(file *common.File) (string, error) {
	path := p.LocalPath(file)

	fr, err := os.Open(path)
	if err != nil {
		log.Error().Err(err).Msgf("cannot read file, file=%s", path)
		return "", err
	}
	defer fr.Close()

	r := bufio.NewScanner(fr)
	for r.Scan() {
		line := r.Text()
		file.Body = append(file.Body, line)
	}

	if err = r.Err(); err != nil {
		log.Error().Err(err).Msgf("cannot read file, file=%s", path)
		return "", err
	}

	log.Debug().Msgf("read file successfully, file=%s", path)

	return path, nil
}

// Abort 放弃当前文件的持久化工作.
func (p *LocalStorage) Abort(file *common.File) error {
	return nil
}

// Delete 从本地磁盘上删除当前文件.
func (p *LocalStorage) Delete(file *common.File) error {
	path := p.LocalPath(file)

	if err := os.Remove(path); err != nil {
		log.Error().Err(err).Msgf("cannot delete file, file=%s", path)
		return err
	}

	return nil
}
