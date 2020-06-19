package pipeline

import (
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	pb "github.com/amazingchow/engine-vector-space-search-service/api"
	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
	conf "github.com/amazingchow/engine-vector-space-search-service/internal/config"
	"github.com/amazingchow/engine-vector-space-search-service/internal/indexing"
	"github.com/amazingchow/engine-vector-space-search-service/internal/kafka"
	"github.com/amazingchow/engine-vector-space-search-service/internal/parse"
	"github.com/amazingchow/engine-vector-space-search-service/internal/stemming"
	"github.com/amazingchow/engine-vector-space-search-service/internal/stopword"
	"github.com/amazingchow/engine-vector-space-search-service/internal/storage"
	"github.com/amazingchow/engine-vector-space-search-service/internal/tokenize"
)

// MOFRPCContainer 中华人民共和国财政部网站文章语料数据容器.
type MOFRPCContainer struct {
	once sync.Once
	cfg  *conf.PipelineConfig

	consumer *kafka.CustomConsumerGroupHandler
	storage  storage.Persister

	parser         *parse.PipeParseProcessor
	parserInput    common.PacketChannel
	tokenizer      *tokenize.PipeTokenizeProcessor
	tokenizerInput common.PacketChannel
	stoper         *stopword.PipeStopWordsProcessor
	stoperInput    common.ConcordanceChannel
	stemmer        *stemming.PipeStemmingProcessor
	stemmerInput   common.ConcordanceChannel
	indexer        *indexing.PipeIndexProcessor
	indexerInput   common.ConcordanceChannel

	pGroup *sync.WaitGroup
	exit   chan struct{}
}

// NewMOFRPCContainer 新建MOFRPC数据容器.
func NewMOFRPCContainer(cfg *conf.PipelineConfig) (*MOFRPCContainer, error) {
	h := &MOFRPCContainer{
		cfg: cfg,
	}

	var err error

	h.consumer, err = kafka.NewCustomConsumerGroupHandler(h.cfg.Kafka)
	if err != nil {
		return nil, err
	}

	h.storage, err = storage.NewS3Storage(h.cfg.Minio)
	if err != nil {
		return nil, err
	}
	h.storage.Init()

	h.parser = parse.NewPipeParseProcessor(h.storage)
	h.parserInput = make(common.PacketChannel)
	h.tokenizer = tokenize.NewPipeTokenizeProcessor(h.storage, common.LanguageTypeChinsese)
	h.tokenizerInput = make(common.PacketChannel)
	h.stoper = stopword.NewPipeStopWordsProcessor(common.LanguageTypeChinsese)
	h.stoperInput = make(common.ConcordanceChannel)
	h.stemmer = stemming.NewPipeStemmingProcessor(common.LanguageTypeChinsese)
	h.stemmerInput = make(common.ConcordanceChannel)
	h.indexer = indexing.NewPipeIndexProcessor(h.cfg.Indexer, h.storage)
	h.indexerInput = make(common.ConcordanceChannel)

	h.pGroup = new(sync.WaitGroup)
	h.exit = make(chan struct{})

	go h.process()

	return h, nil
}

func (h *MOFRPCContainer) process() {
	go h.parser.InfoExtract(h.pGroup, h.parserInput, h.tokenizerInput)
	go h.tokenizer.InfoTokenize(h.pGroup, h.tokenizerInput, h.stoperInput)
	go h.stoper.RemoveStopWords(h.pGroup, h.stoperInput, h.stemmerInput)
	go h.stemmer.ApplyStemming(h.pGroup, h.stemmerInput, h.indexerInput)
	go h.indexer.TermsIndexing(h.pGroup, h.indexerInput)
}

// Run 运行MOFRPC数据容器.
func (h *MOFRPCContainer) Run() {
LOOP:
	for {
		select {
		case msg, ok := <-h.consumer.Msg():
			{
				if !ok {
					break LOOP
				}
				packet := new(pb.Packet)
				if err := proto.Unmarshal(msg.Value, packet); err != nil {
					log.Warn().Err(err).Msg("bad message")
					continue
				}
				h.parserInput <- packet
				time.Sleep(time.Second)
			}
		}
	}
	h.exit <- struct{}{}
}

// Stop 停止运行MOFRPC数据容器.
func (h *MOFRPCContainer) Stop() {
	log.Info().Msg("try to close pipeline container ...")
	h.once.Do(func() {
		log.Info().Msg("try to close kafka consumer ...")
		h.consumer.Close()
		log.Info().Msg("try to close all nlp processors ...")
		close(h.parserInput)
		h.pGroup.Wait()
		log.Info().Msg("try to close minio client ...")
		h.indexer.Dump()
		log.Info().Msg("try to dump terms indexing ...")
		h.storage.Destroy()
	})
	<-h.exit
	log.Info().Msg("pipeline container has been closed")
}
