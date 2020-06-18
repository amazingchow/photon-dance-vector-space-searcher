package indexing

import (
	"io/ioutil"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"

	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
	"github.com/amazingchow/engine-vector-space-search-service/internal/storage"
)

// PipeIndexProcessor 索引器.
type PipeIndexProcessor struct {
	TokenBucket chan struct{}
	Indexer     InvertedIndex
	Storage     storage.Persister
}

// InvertedIndex 倒排索引数据结构.
type InvertedIndex struct {
	mu   sync.Mutex
	Dict map[string]*PostingList `json:"dict"`
}

// PostingList 信息列表.
type PostingList struct {
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
func NewPipeIndexProcessor(storage storage.Persister) *PipeIndexProcessor {
	p := &PipeIndexProcessor{
		TokenBucket: make(chan struct{}, 20),
		Storage:     storage,
	}
	p.Indexer = InvertedIndex{
		Dict: make(map[string]*PostingList),
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
	p.TokenBucket <- struct{}{}

	for term, freq := range packet.Concordance {
		list, ok := p.Indexer.Dict[term]
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
			p.Indexer.Dict[term].Postings = cur
			p.Indexer.Dict[term].DocFrequency++
		} else {
			p.Indexer.Dict[term] = &PostingList{
				DocFrequency: 1,
				Postings: &Posting{
					Next: nil,
				},
			}
			p.Indexer.Dict[term].Postings.Next = &Posting{
				TermFrequency: freq,
				DocID:         packet.DocID,
				Next:          nil,
			}
		}
	}

	<-p.TokenBucket
}

// Dump 将索引结构持久化到存储硬件.
func (p *PipeIndexProcessor) Dump() {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	serialization, err := json.Marshal(&(p.Indexer))
	if err != nil {
		log.Fatal().Err(err).Msg("cannot dump terms indexing")
	}
	if err = ioutil.WriteFile("index.json", serialization, 0644); err != nil {
		log.Fatal().Err(err).Msg("cannot dump terms indexing")
	}
}

// Load 从存储硬件加载索引结构.
func (p *PipeIndexProcessor) Load() {
	deserialization, err := ioutil.ReadFile("index.json")
	if err != nil {
		log.Fatal().Err(err).Msg("cannot load terms indexing")
	}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(deserialization, &(p.Indexer)); err != nil {
		log.Fatal().Err(err).Msg("cannot load terms indexing")
	}
}
