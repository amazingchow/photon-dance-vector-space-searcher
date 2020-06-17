package storage

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/minio/minio-go"
	"github.com/rs/zerolog/log"

	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
	conf "github.com/amazingchow/engine-vector-space-search-service/internal/config"
	"github.com/amazingchow/engine-vector-space-search-service/internal/utils"
)

// S3Storage 提供s3持久化服务.
type S3Storage struct {
	cfg *conf.MinioConfig
	cli *minio.Client
	tmp string
}

// NewS3Storage 返回s3持久化服务实例.
// For Unix system, path should meet unix naming rules, like "/path/to/a/b/c".
func NewS3Storage(cfg *conf.MinioConfig) (*S3Storage, error) {
	cli, err := minio.New(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.UseSSL)
	if err != nil {
		log.Error().Err(err).Msg("cannot create s3 client")
		return nil, err
	}

	tmpDir, err := ioutil.TempDir("", "engine-vector-space-search-service")
	if err != nil {
		log.Error().Err(err).Msg("cannot create temporary dir '/tmp/engine-vector-space-search-service'")
		return nil, err
	}

	return &S3Storage{
		cfg: cfg,
		cli: cli,
		tmp: tmpDir,
	}, nil
}

// Init 初始化用于s3持久化服务的资源.
func (p *S3Storage) Init() error {
	ok, err := p.cli.BucketExists(p.cfg.Bucket)
	if err != nil || !ok {
		log.Error().Err(err).Msgf("bucket <%s> not exist", p.cfg.Bucket)
		return fmt.Errorf("bucket <%s> not exist", p.cfg.Bucket)
	}
	log.Info().Msg("load minio plugin")
	return nil
}

// Destroy 清除s3持久化服务的资源.
func (p *S3Storage) Destroy() error {
	log.Info().Msg("unload minio plugin")
	return os.RemoveAll(p.tmp)
}

// RemotePath 服务端文件持久化路径.
func (p *S3Storage) RemotePath(file *common.File) string {
	return filepath.Join(p.cfg.Root, fmt.Sprintf("%s/%s.%s",
		common.FileType2FileTypeName[file.Type], file.Name, common.FileType2FileSuffix[file.Type]))
}

// LocalPath 本地文件暂存路径.
func (p *S3Storage) LocalPath(file *common.File) string {
	return filepath.Join(p.tmp, fmt.Sprintf("%s.%s",
		file.Name, common.FileType2FileSuffix[file.Type]))
}

// Writable 检查当前文件是否可写, 可以就写入本地暂存磁盘, 并返回写入的本地暂存路径.
func (p *S3Storage) Writable(file *common.File) (string, error) {
	lPath := p.LocalPath(file)

	fw, err := os.Create(lPath)
	if err != nil {
		log.Error().Err(err).Msgf("cannot write local tmp file, file=%s", lPath)
		return "", err
	}
	defer fw.Close()

	w := bufio.NewWriter(fw)
	defer w.Flush()

	for idx, line := range file.Body {
		if idx == len(file.Body)-1 {
			if _, err = w.WriteString(line); err != nil {
				log.Error().Err(err).Msgf("cannot write local tmp file, file=%s", lPath)
				return "", err
			}
		} else {
			if _, err = w.WriteString(line + "\n"); err != nil {
				log.Error().Err(err).Msgf("cannot write local tmp file, file=%s", lPath)
				return "", err
			}
		}
	}

	return lPath, nil
}

// Put 将本地暂存磁盘上的文件写入s3集群.
func (p *S3Storage) Put(file *common.File) (string, error) {
	rPath := p.RemotePath(file)
	lPath := p.LocalPath(file)

	retry := 0
	operation := func() error {
		n, err := p.cli.FPutObject(p.cfg.Bucket, rPath, lPath, minio.PutObjectOptions{})
		if err != nil {
			log.Warn().Err(err).Msgf("cannot write local tmp file to s3, retry=%d, object=%s, file=%s, file size=%d, uploaded=%d",
				retry, rPath, lPath, utils.FileSize(lPath), n)
			retry++
			return err
		}
		return nil
	}

	notify := func(err error, sec time.Duration) {
		if err != nil {
			log.Info().Msgf("will retry in %.1fs", sec.Seconds())
		}
	}

	if err := backoff.RetryNotify(operation, utils.BackoffPolicy(), notify); err != nil {
		return "", err
	}

	log.Debug().Msgf("write local tmp file to s3, object=%s", rPath)

	return lPath, nil
}

// Readable 检查当前文件是否可读, 可以就将s3集群上的文件写入本地暂存磁盘, 并返回写入的本地暂存路径.
func (p *S3Storage) Readable(file *common.File) (string, error) {
	rPath := p.RemotePath(file)
	lPath := p.LocalPath(file)

	var obj *minio.Object

	retry := 0
	operation := func() error {
		var err error
		obj, err = p.cli.GetObject(p.cfg.Bucket, rPath, minio.GetObjectOptions{})
		if err != nil {
			log.Warn().Err(err).Msgf("cannot read remote file from s3, retry=%d, object=%s", retry, rPath)
			retry++
			return err
		}
		return nil
	}

	notify := func(err error, sec time.Duration) {
		if err != nil {
			log.Info().Msgf("will retry in %.1fs", sec.Seconds())
		}
	}

	if err := backoff.RetryNotify(operation, utils.BackoffPolicy(), notify); err != nil {
		return "", err
	}

	fw, err := os.Create(lPath)
	if err != nil {
		log.Error().Err(err).Msgf("cannot create local tmp file, file=%s", lPath)
		return "", err
	}
	defer fw.Close()

	if _, err = io.Copy(fw, obj); err != nil {
		log.Error().Err(err).Msgf("cannot copy remote file to local tmp file, object=%s, file=%s", rPath, lPath)
		return "", err
	}

	log.Debug().Msgf("read remote file from s3, object=%s", rPath)

	return lPath, nil
}

// Get 从本地暂存磁盘上读取当前文件.
func (p *S3Storage) Get(file *common.File) (string, error) {
	lPath := p.LocalPath(file)

	fr, err := os.Open(lPath)
	if err != nil {
		log.Error().Err(err).Msgf("cannot read local tmp file, file=%s", lPath)
		return "", err
	}
	defer fr.Close()

	r := bufio.NewScanner(fr)
	for r.Scan() {
		line := r.Text()
		file.Body = append(file.Body, line)
	}

	if err = r.Err(); err != nil {
		log.Error().Err(err).Msgf("cannot read local tmp file, file=%s", lPath)
		return "", err
	}

	return lPath, nil
}

// Abort 从本地暂存磁盘上删除当前文件.
func (p *S3Storage) Abort(file *common.File) error {
	lPath := p.LocalPath(file)

	if err := os.Remove(lPath); err != nil {
		log.Warn().Err(err).Msgf("cannot delete local tmp file, file=%s", lPath)
		return err
	}

	return nil
}

// Delete 从s3集群上删除当前文件.
func (p *S3Storage) Delete(file *common.File) error {
	rPath := p.RemotePath(file)

	retry := 0
	operation := func() error {
		if err := p.cli.RemoveObject(p.cfg.Bucket, rPath); err != nil {
			log.Warn().Err(err).Msgf("cannot delete remote file from s3, retry=%d, object=%s", retry, rPath)
			retry++
			return err
		}
		return nil
	}

	notify := func(err error, sec time.Duration) {
		if err != nil {
			log.Info().Msgf("will retry in %.1fs", sec.Seconds())
		}
	}

	if err := backoff.RetryNotify(operation, utils.BackoffPolicy(), notify); err != nil {
		return err
	}

	return nil
}
