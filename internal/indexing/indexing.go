package indexing

import (
	"container/heap"
	"fmt"
	"io/ioutil"
	"math"
	"math/bits"
	"strconv"
	"sync"
	"sync/atomic"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"

	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
	conf "github.com/amazingchow/engine-vector-space-search-service/internal/config"
	"github.com/amazingchow/engine-vector-space-search-service/internal/storage"
	"github.com/amazingchow/engine-vector-space-search-service/internal/utils"
)

const (
	// DocCapacity 支持的最大文档总量
	DocCapacity uint64 = 1e4
	// VocabularyCapacity 支持的最大词汇总量
	VocabularyCapacity uint64 = 1e5

	_BitPerWord uint64 = 64
	_Shift      uint64 = 6
	_Mask       uint64 = 0x3f
)

// PipeIndexProcessor 索引器
// 目前规划最大支持10万词汇量, 1万文档.
type PipeIndexProcessor struct {
	cfg         *conf.IndexerConfig
	tokenBucket chan struct{}
	indexer     InvertedIndex
	tfidf       TFIDF
	storage     storage.Persister
	available   int32
}

// InvertedIndex 倒排索引数据结构
type InvertedIndex struct {
	mu               sync.Mutex
	Doc              uint64                  `json:"doc"`
	DocStore         *DocStore               `json:"doc_store"`
	Vocabulary       uint64                  `json:"vocabulary"`
	VocabularyStore  *VocabularyStore        `json:"vocabulary_store"`
	Dict             map[string]*PostingList `json:"dict"`
	MaxTermFrequency uint64                  `json:"max_term_frequency"`
}

// DocStore 用于存储文档记录
type DocStore struct {
	BitSet []uint64 `json:"bit_set"`
}

// VocabularyStore 用于存储词汇量记录
type VocabularyStore struct {
	BitSet []uint64 `json:"bit_set"`
}

// PostingList 信息列表
type PostingList struct {
	TermID       string   `json:"term_id"`
	DocFrequency uint64   `json:"doc_frequency"`
	Postings     *Posting `json:"postings"`
}

// Posting 信息单元
type Posting struct {
	TermFrequency uint64   `json:"term_frequency"`
	DocID         string   `json:"doc_id"`
	Next          *Posting `json:"next"`
}

// TFIDF TF-IDF数据结构
type TFIDF struct {
	Vectors []*DocVector
}

// DocVector 文档向量
type DocVector struct {
	DocID string
	Space []float32
}

// QueryVector 查询向量
type QueryVector struct {
	Space []float32
}

// SimilarObject 相似文档记录
type SimilarObject struct {
	DocID      string
	Similarity float64
	Index      int
}

// PriorityQueue 用于筛选TopK文档的优先队列
type PriorityQueue []*SimilarObject

// NewPipeIndexProcessor 新建索引器.
func NewPipeIndexProcessor(cfg *conf.IndexerConfig, storage storage.Persister) *PipeIndexProcessor {
	p := &PipeIndexProcessor{
		cfg:         cfg,
		tokenBucket: make(chan struct{}, 20),
		storage:     storage,
		available:   0,
	}
	p.indexer = InvertedIndex{
		Doc:             0,
		DocStore:        &DocStore{BitSet: make([]uint64, uint64(DocCapacity/_BitPerWord)+1)},
		Vocabulary:      0,
		VocabularyStore: &VocabularyStore{BitSet: make([]uint64, uint64(VocabularyCapacity/_BitPerWord)+1)},
		Dict:            make(map[string]*PostingList),
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

	if p.indexer.DocStore.exist(packet.DocID) {
		return
	}
	p.indexer.DocStore.set(packet.DocID)
	p.indexer.Doc++

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
			termID := fmt.Sprintf("%010d", p.indexer.Vocabulary)
			p.indexer.VocabularyStore.set(termID)
			p.indexer.Vocabulary++

			p.indexer.Dict[term] = &PostingList{
				TermID:       termID,
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

// MarkServiceAvailable 将服务标记为可用.
func (p *PipeIndexProcessor) MarkServiceAvailable() {
	atomic.StoreInt32(&(p.available), 1)
}

// MarkServiceUnavailable 将服务标记为不可用.
func (p *PipeIndexProcessor) MarkServiceUnavailable() {
	atomic.StoreInt32(&(p.available), 0)
}

// ServiceAvailable 服务是否可用.
func (p *PipeIndexProcessor) ServiceAvailable() bool {
	return atomic.LoadInt32(&(p.available)) == 1
}

// BuildTFIDF 构造TF-IDF数据结构.
func (p *PipeIndexProcessor) BuildTFIDF() {
	p.tfidf = TFIDF{
		Vectors: make([]*DocVector, p.indexer.Doc),
	}
	var i uint64
	for i = 0; i < p.indexer.Doc; i++ {
		p.tfidf.Vectors[i] = &DocVector{
			Space: make([]float32, p.indexer.Vocabulary),
		}
	}
	D := p.indexer.Doc
	for _, pl := range p.indexer.Dict {
		termIdx, _ := strconv.ParseUint(pl.TermID, 10, 64)
		for cur := pl.Postings.Next; cur != nil; cur = cur.Next {
			docIdx, _ := strconv.ParseUint(cur.DocID, 10, 64)
			p.tfidf.Vectors[docIdx].DocID = cur.DocID
			p.tfidf.Vectors[docIdx].Space[termIdx] = float32(cur.TermFrequency) * float32(math.Log2(float64(D)/float64(pl.DocFrequency)))
		}
		if cur := pl.Postings.Next; cur != nil {
			if cur.TermFrequency > p.indexer.MaxTermFrequency {
				p.indexer.MaxTermFrequency = cur.TermFrequency
			}
		}
	}
}

// BuildQueryVector 构造查询向量.
func (p *PipeIndexProcessor) BuildQueryVector(concordance map[string]uint32) *QueryVector {
	q := &QueryVector{
		Space: make([]float32, p.indexer.Vocabulary),
	}
	D := p.indexer.Doc
	for term := range concordance {
		if pl, ok := p.indexer.Dict[term]; ok {
			termIdx, _ := strconv.ParseUint(pl.TermID, 10, 64)
			for cur := pl.Postings.Next; cur != nil; cur = cur.Next {
				q.Space[termIdx] = float32(cur.TermFrequency) * float32(math.Log2(float64(D)/float64(pl.DocFrequency)))
				q.Space[termIdx] = (0.5 + (0.5*float32(cur.TermFrequency))/float32(p.indexer.MaxTermFrequency)) * float32(math.Log2(float64(D)/float64(pl.DocFrequency)))
			}
		}
	}
	return q
}

// TopK 计算查询向量与文档向量集合中各个向量的相似度，并返回最相似的k个文档
func (p *PipeIndexProcessor) TopK(k int, q *QueryVector) []string {
	var qMagnitude float64
	for _, x := range q.Space {
		qMagnitude += float64(x) * float64(x)
	}
	qMagnitude = math.Sqrt(qMagnitude)

	h := new(PriorityQueue)
	heap.Init(h)

	var dot float64
	var dMagnitude float64
	var similarity float64
	for _, v := range p.tfidf.Vectors {
		dot = 0.0
		dMagnitude = 0.0
		for i := range v.Space {
			dot += float64(v.Space[i]) * float64(q.Space[i])
			dMagnitude += float64(v.Space[i]) * float64(v.Space[i])
		}
		dMagnitude = math.Sqrt(dMagnitude)
		similarity = dot / (dMagnitude * qMagnitude)

		if h.Len() >= k {
			if h.Top().(*SimilarObject).Similarity < similarity {
				h.Pop()
				h.Push(&SimilarObject{DocID: v.DocID, Similarity: similarity})
			}
		} else {
			h.Push(&SimilarObject{DocID: v.DocID, Similarity: similarity})
		}
	}

	ret := make([]string, k)
	for h.Len() > 0 {
		ret = append(ret, h.Pop().(*SimilarObject).DocID)
	}

	return ret
}

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Similarity < pq[j].Similarity
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i].DocID, pq[j].DocID = pq[j].DocID, pq[i].DocID
	pq[i].Similarity, pq[j].Similarity = pq[j].Similarity, pq[i].Similarity
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*SimilarObject)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.Index = -1
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) Top() interface{} {
	n := len(*pq)
	item := (*pq)[n-1]
	return item
}

func (m *DocStore) set(docID string) {
	i, _ := strconv.ParseUint(docID, 10, 64)
	m.BitSet[i>>_Shift] |= (1 << (i & _Mask))
}

func (m *DocStore) clear(docID string) {
	i, _ := strconv.ParseUint(docID, 10, 64)
	m.BitSet[i>>_Shift] &= bits.Reverse64(1 << (i & _Mask))
}

func (m *DocStore) exist(docID string) bool {
	i, _ := strconv.ParseUint(docID, 10, 64)
	return m.BitSet[i>>_Shift]&(1<<(i&_Mask)) != 0
}

func (m *VocabularyStore) set(termID string) {
	i, _ := strconv.ParseUint(termID, 10, 64)
	m.BitSet[i>>_Shift] |= (1 << (i & _Mask))
}

func (m *VocabularyStore) clear(termID string) {
	i, _ := strconv.ParseUint(termID, 10, 64)
	m.BitSet[i>>_Shift] &= bits.Reverse64(1 << (i & _Mask))
}

func (m *VocabularyStore) exist(termID string) bool {
	i, _ := strconv.ParseUint(termID, 10, 64)
	return m.BitSet[i>>_Shift]&(1<<(i&_Mask)) != 0
}
