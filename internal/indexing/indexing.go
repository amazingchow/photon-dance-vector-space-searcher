package indexing

import (
	"fmt"
	"io/ioutil"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"

	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
	conf "github.com/amazingchow/engine-vector-space-search-service/internal/config"
	"github.com/amazingchow/engine-vector-space-search-service/internal/storage"
	"github.com/amazingchow/engine-vector-space-search-service/internal/utils"
)

// PipeIndexProcessor 索引器.
type PipeIndexProcessor struct {
	cfg         *conf.IndexerConfig
	tokenBucket chan struct{}
	indexer     InvertedIndex
	storage     storage.Persister
}

// InvertedIndex 倒排索引数据结构.
type InvertedIndex struct {
	mu         sync.Mutex
	Vocabulary uint32
	Dict       map[string]*PostingList `json:"dict"`
}

// PostingList 信息列表.
type PostingList struct {
	TermID       string   `json:"term_id"`
	DocFrequency uint32   `json:"doc_frequency"`
	Postings     *Posting `json:"postings"`
}

// Posting 信息单元.
type Posting struct {
	TermFrequency uint32   `json:"term_frequency"`
	DocID         string   `json:"doc_id"`
	Next          *Posting `json:"next"`
}

// NewPipeIndexProcessor 新建索引器.
func NewPipeIndexProcessor(cfg *conf.IndexerConfig, storage storage.Persister) *PipeIndexProcessor {
	p := &PipeIndexProcessor{
		cfg:         cfg,
		tokenBucket: make(chan struct{}, 20),
		storage:     storage,
	}
	p.indexer = InvertedIndex{
		Vocabulary: 0,
		Dict:       make(map[string]*PostingList),
	}
	log.Info().Msg("load PipeIndexProcessor plugin")
	return p
}

// TermsIndexing 为词条建立索引结构.
func (p *PipeIndexProcessor) TermsIndexing(pGroup *sync.WaitGroup, input common.ConcordanceChannel) {
	pGroup.Add(1)
LOOP_LABEL:
	for {
		select {
		case packet, ok := <-input:
			{
				if !ok {
					break LOOP_LABEL
				}
				go p.indexing(packet)
			}
		}
	}
	pGroup.Done()
	log.Info().Msg("unload PipeIndexProcessor plugin")
}

func (p *PipeIndexProcessor) indexing(packet *common.ConcordanceWrapper) {
	p.tokenBucket <- struct{}{}

	for term, freq := range packet.Concordance {
		list, ok := p.indexer.Dict[term]
		if ok {
			cur := list.Postings
			inserted := false
			for ; cur.Next != nil; cur = cur.Next {
				if freq >= cur.Next.TermFrequency {
					tmp := cur.Next
					cur.Next = &Posting{
						TermFrequency: freq,
						DocID:         packet.DocID,
						Next:          nil,
					}
					cur.Next.Next = tmp
					inserted = true
					break
				}
			}
			if !inserted {
				cur.Next = &Posting{
					TermFrequency: freq,
					DocID:         packet.DocID,
					Next:          nil,
				}
			}
			p.indexer.Dict[term].Postings = cur
			p.indexer.Dict[term].DocFrequency++
		} else {
			p.indexer.Vocabulary++
			p.indexer.Dict[term] = &PostingList{
				TermID:       fmt.Sprintf("%010d", p.indexer.Vocabulary),
				DocFrequency: 1,
				Postings: &Posting{
					Next: nil,
				},
			}
			p.indexer.Dict[term].Postings.Next = &Posting{
				TermFrequency: freq,
				DocID:         packet.DocID,
				Next:          nil,
			}
		}
	}

	<-p.tokenBucket
}

// Dump 将索引结构持久化到存储硬件.
func (p *PipeIndexProcessor) Dump() {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	serialization, err := json.Marshal(&(p.indexer))
	if err != nil {
		log.Fatal().Err(err).Msg("cannot dump terms indexing")
	}
	if err = ioutil.WriteFile(p.cfg.DumpPath, serialization, 0644); err != nil {
		log.Fatal().Err(err).Msg("cannot dump terms indexing")
	}
	log.Info().Msgf("dump terms indexing to file=%s", p.cfg.DumpPath)
}

// Load 从存储硬件加载索引结构.
func (p *PipeIndexProcessor) Load() {
	if utils.FileExist(p.cfg.DumpPath) {
		deserialization, err := ioutil.ReadFile(p.cfg.DumpPath)
		if err != nil {
			log.Fatal().Err(err).Msg("cannot load terms indexing")
		}
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		if err = json.Unmarshal(deserialization, &(p.indexer)); err != nil {
			log.Fatal().Err(err).Msg("cannot load terms indexing")
		}
		log.Info().Msgf("load terms indexing from file=%s", p.cfg.DumpPath)
	}
}
