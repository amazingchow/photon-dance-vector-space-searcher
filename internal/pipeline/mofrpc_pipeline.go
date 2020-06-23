package pipeline

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	pb "github.com/amazingchow/engine-vector-space-search-service/api"
	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
	conf "github.com/amazingchow/engine-vector-space-search-service/internal/config"
	"github.com/amazingchow/engine-vector-space-search-service/internal/indexing"
	"github.com/amazingchow/engine-vector-space-search-service/internal/kafka"
	"github.com/amazingchow/engine-vector-space-search-service/internal/mysql"
	"github.com/amazingchow/engine-vector-space-search-service/internal/parse"
	"github.com/amazingchow/engine-vector-space-search-service/internal/stemming"
	"github.com/amazingchow/engine-vector-space-search-service/internal/stopword"
	"github.com/amazingchow/engine-vector-space-search-service/internal/storage"
	"github.com/amazingchow/engine-vector-space-search-service/internal/tokenize"
	"github.com/amazingchow/engine-vector-space-search-service/internal/utils"
)

// MOFRPCContainer 中华人民共和国财政部网站文章语料数据容器.
type MOFRPCContainer struct {
	once sync.Once
	cfg  *conf.PipelineConfig

	consumer *kafka.CustomConsumerGroupHandler
	storage  storage.Persister
	db       *mysql.Client

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

// NewMOFRPCContainer 新建MOF-RPC数据容器.
func NewMOFRPCContainer(cfg *conf.PipelineConfig) *MOFRPCContainer {
	h := &MOFRPCContainer{
		cfg: cfg,
	}

	var err error

	h.consumer, err = kafka.NewCustomConsumerGroupHandler(h.cfg.Kafka)
	if err != nil {
		log.Fatal().Err(err)
	}

	h.storage, err = storage.NewS3Storage(h.cfg.Minio)
	if err != nil {
		log.Fatal().Err(err)
	}
	if err = h.storage.Init(); err != nil {
		log.Fatal().Err(err)
	}

	h.db = mysql.NewClient(h.cfg.MySQL)
	if err = h.db.Setup(); err != nil {
		log.Fatal().Err(err)
	}

	h.parser = parse.NewPipeParseProcessor(h.storage)
	h.parserInput = make(common.PacketChannel, 20)
	h.tokenizer = tokenize.NewPipeTokenizeProcessor(h.storage, common.LanguageTypeChinsese)
	h.tokenizerInput = make(common.PacketChannel, 20)
	h.stoper = stopword.NewPipeStopWordsProcessor(common.LanguageTypeChinsese)
	h.stoperInput = make(common.ConcordanceChannel, 20)
	h.stemmer = stemming.NewPipeStemmingProcessor(common.LanguageTypeChinsese)
	h.stemmerInput = make(common.ConcordanceChannel, 20)
	h.indexer = indexing.NewPipeIndexProcessor(h.cfg.Indexer, h.storage)
	h.indexerInput = make(common.ConcordanceChannel, 20)

	h.pGroup = new(sync.WaitGroup)
	h.exit = make(chan struct{})

	go h.process(cfg.Indexer.Load)

	return h
}

func (h *MOFRPCContainer) process(load bool) {
	if load {
		h.indexer.MarkServiceUnavailable()
		h.indexer.Load()
		h.indexer.BuildTFIDF()
		h.indexer.MarkServiceAvailable()
	}
	go h.parser.InfoExtract(h.pGroup, h.parserInput, h.tokenizerInput)
	go h.tokenizer.InfoTokenize(h.pGroup, h.tokenizerInput, h.stoperInput)
	go h.stoper.RemoveStopWords(h.pGroup, h.stoperInput, h.stemmerInput)
	go h.stemmer.ApplyStemming(h.pGroup, h.stemmerInput, h.indexerInput)
	go h.indexer.TermsIndexing(h.pGroup, h.indexerInput)
}

// Run 运行MOF-RPC数据容器.
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
				if packet.DeliveryStatus == pb.PacketDeliveryStatus_InDelivery {
					log.Info().Msg("consume one packet")
					h.indexer.MarkServiceUnavailable()
					h.parserInput <- packet
				} else if packet.DeliveryStatus == pb.PacketDeliveryStatus_OutOfStock {
					h.indexer.BuildTFIDF()
					h.indexer.MarkServiceAvailable()
				}
			}
		default:
			{

			}
		}
	}
	h.exit <- struct{}{}
}

// Stop 停止运行MOF-RPC数据容器 (并发安全).
func (h *MOFRPCContainer) Stop() {
	log.Info().Msg("try to close pipeline container ...")
	h.once.Do(func() {
		h.consumer.Close()
		close(h.parserInput)
		h.pGroup.Wait()
		h.indexer.Dump()
		h.storage.Destroy() // nolint
		h.db.Close()        // nolint
	})
	<-h.exit
	log.Info().Msg("pipeline container has been closed")
}

// Query 利用关键词查询相似文档.
func (h *MOFRPCContainer) Query(ctx context.Context, topk uint32, query string) ([]string, error) {
	if !h.indexer.ServiceAvailable() {
		return nil, utils.ErrServiceUnavailable
	}

	concordance := make(map[string]uint64)

	h.tokenizer.QueryTokenize(query, common.LanguageTypeChinsese, concordance)
	if utils.IsContextDone(ctx) {
		return nil, utils.ErrContextDone
	}

	h.stoper.QueryRemoveStopWords(common.LanguageTypeChinsese, concordance)
	if utils.IsContextDone(ctx) {
		return nil, utils.ErrContextDone
	}

	h.stemmer.QueryApplyStemming(common.LanguageTypeChinsese, concordance)
	if utils.IsContextDone(ctx) {
		return nil, utils.ErrContextDone
	}

	q := h.indexer.BuildQueryVector(concordance)
	if utils.IsContextDone(ctx) {
		return nil, utils.ErrContextDone
	}

	docIDList := h.indexer.TopK(topk, q)
	if utils.IsContextDone(ctx) {
		return nil, utils.ErrContextDone
	}

	docTitileList := make([]string, len(docIDList))
	for idx, id := range docIDList {
		docTitileList[idx] = h.db.Query(id)
	}

	return docTitileList, nil
}

// GetSystemInfo 获取系统信息.
func (h *MOFRPCContainer) GetSystemInfo() (*pb.GetSystemInfoResponse, error) {
	if !h.indexer.ServiceAvailable() {
		return nil, utils.ErrServiceUnavailable
	}

	return &pb.GetSystemInfoResponse{
		DocumentCapacity:   h.indexer.GetDocCapacity(),
		Document:           h.indexer.GetDoc(),
		VocabularyCapacity: h.indexer.GetVocabularyCapacity(),
		Vocabulary:         h.indexer.GetVocabulary(),
		ServiceStatus:      pb.ServiceStatus_Available,
	}, nil
}
