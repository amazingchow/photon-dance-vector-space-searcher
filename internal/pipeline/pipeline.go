package pipeline

import (
	conf "github.com/amazingchow/engine-vector-space-search-service/internal/config"
	"github.com/amazingchow/engine-vector-space-search-service/internal/kafka"
	"github.com/amazingchow/engine-vector-space-search-service/internal/parse"
	"github.com/amazingchow/engine-vector-space-search-service/internal/stemming"
	"github.com/amazingchow/engine-vector-space-search-service/internal/stopword"
	"github.com/amazingchow/engine-vector-space-search-service/internal/storage"
	"github.com/amazingchow/engine-vector-space-search-service/internal/tokenize"
)

// Container 数据容器.
type Container struct {
	cfg       *conf.PipelineConfig
	consumer  *kafka.ConsumerHandler
	storage   storage.Persister
	parser    *parse.PipeParseProcessor
	tokenizer *tokenize.PipeTokenizeProcessor
	stoper    *stopword.PipeStopWordsProcessor
	stemmer   *stemming.PipeStemmingProcessor
}

// NewContainer 新建数据容器.
func NewContainer(cfg *conf.PipelineConfig) (*Container, error) {
	h := &Container{
		cfg: cfg,
	}

	var err error

	h.consumer, err = kafka.NewConsumerHandler(h.cfg.Kafka)
	if err != nil {
		return nil, err
	}

	h.storage, err = storage.NewS3Storage(h.cfg.Minio)
	if err != nil {
		return nil, err
	}

	h.parser = parse.NewPipeParseProcessor(h.storage)
	h.tokenizer = tokenize.NewPipeTokenizeProcessor(h.storage)
	h.stoper = stopword.NewPipeStopWordsProcessor()
	h.stemmer = stemming.NewPipeStemmingProcessor()

	return h, nil
}
