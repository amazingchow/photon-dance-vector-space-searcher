package indexing

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"

	"github.com/amazingchow/photon-dance-vector-space-searcher/internal/common"
	conf "github.com/amazingchow/photon-dance-vector-space-searcher/internal/config"
	"github.com/amazingchow/photon-dance-vector-space-searcher/internal/storage"
	"github.com/amazingchow/photon-dance-vector-space-searcher/internal/utils"
)

const (
	// 支持的文档总量上限
	_DocCapacity uint64 = 1e4
	// 支持的词汇总量上限
	_VocabularyCapacity uint64 = 1e5

	_Shards = 32

	_BitPerWord uint64 = 64
	_Shift      uint64 = 6
	_Mask       uint64 = 0x3f
)

// PipeIndexProcessor 索引器
// 目前规划最大支持10万词汇量, 1万文档.
type PipeIndexProcessor struct {
	cfg         *conf.IndexerConfig
	tokenBucket chan struct{}
	indexer     *InvertedIndex
	tfidf       *TFIDF
	storage     storage.Persister
	available   int32
}

// InvertedIndex 倒排索引数据结构
type InvertedIndex struct {
	Metadata *Metadata
	Dict     []*Shard // 使用分段锁技术
}

// Metadata 倒排索引数据结构的元数据
type Metadata struct {
	Doc              uint64           `json:"doc"`
	DocStore         *DocStore        `json:"doc_store"`
	Vocabulary       uint64           `json:"vocabulary"`
	VocabularyStore  *VocabularyStore `json:"vocabulary_store"`
	MaxTermFrequency uint64           `json:"max_term_frequency"`
}

// Shard 倒排索引数据结构的局部字典
type Shard struct {
	mu      sync.RWMutex
	Backend map[string]*PostingList `json:"backend"`
}

// DocStore 用于存储文档记录
type DocStore struct {
	mu     sync.RWMutex
	BitSet [][]byte `json:"bit_set"`
}

// VocabularyStore 用于存储词汇量记录
type VocabularyStore struct {
	mu     sync.RWMutex
	BitSet [][]byte `json:"bit_set"`
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
	DocIdx        uint64   `json:"doc_idx"`
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
		available:   1,
	}
	p.indexer = &InvertedIndex{}
	p.indexer.Metadata = &Metadata{
		Doc:              0,
		Vocabulary:       0,
		MaxTermFrequency: 0,
	}
	p.indexer.Metadata.DocStore = &DocStore{BitSet: make([][]byte, uint64(_DocCapacity/_BitPerWord)+1)}
	for idx := range p.indexer.Metadata.DocStore.BitSet {
		p.indexer.Metadata.DocStore.BitSet[idx] = make([]byte, 8)
	}
	p.indexer.Metadata.VocabularyStore = &VocabularyStore{BitSet: make([][]byte, uint64(_VocabularyCapacity/_BitPerWord)+1)}
	for idx := range p.indexer.Metadata.VocabularyStore.BitSet {
		p.indexer.Metadata.VocabularyStore.BitSet[idx] = make([]byte, 8)
	}
	p.indexer.Dict = make([]*Shard, _Shards)
	for idx := 0; idx < _Shards; idx++ {
		p.indexer.Dict[idx] = &Shard{
			Backend: make(map[string]*PostingList),
		}
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
		default:
			{

			}
		}
	}
	pGroup.Done()
	log.Info().Msg("unload PipeIndexProcessor plugin")
}

func (p *PipeIndexProcessor) indexing(packet *common.ConcordanceWrapper) {
	p.tokenBucket <- struct{}{}

	if p.indexer.Metadata.DocStore.exist(packet.DocID) {
		return
	}
	p.indexer.Metadata.DocStore.set(packet.DocID)
	atomic.AddUint64(&(p.indexer.Metadata.Doc), 1)

	for term, freq := range packet.Concordance {
		shard := p.indexer.Dict[fnv_1a_32(term)&0x1f]
		shard.mu.Lock()

		docIdx := p.GetDoc()

		_, ok := shard.Backend[term]
		if ok {
			cur := shard.Backend[term].Postings
			inserted := false
			for ; cur.Next != nil; cur = cur.Next {
				if freq > cur.Next.TermFrequency {
					tmp := cur.Next
					cur.Next = &Posting{
						TermFrequency: freq,
						DocIdx:        docIdx,
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
					DocIdx:        docIdx,
					DocID:         packet.DocID,
					Next:          nil,
				}
			}
			shard.Backend[term].Postings = cur
			shard.Backend[term].DocFrequency++
		} else {
			atomic.AddUint64(&(p.indexer.Metadata.Vocabulary), 1)
			termID := fmt.Sprintf("%010d", p.GetVocabulary())
			p.indexer.Metadata.VocabularyStore.set(termID)

			shard.Backend[term] = &PostingList{
				TermID:       termID,
				DocFrequency: 1,
				Postings: &Posting{
					Next: nil,
				},
			}
			shard.Backend[term].Postings.Next = &Posting{
				TermFrequency: freq,
				DocIdx:        docIdx,
				DocID:         packet.DocID,
				Next:          nil,
			}
		}

		shard.mu.Unlock()
		p.indexer.Dict[fnv_1a_32(term)&0x1f] = shard
	}

	<-p.tokenBucket
}

// Dump 将索引结构持久化到存储硬件.
func (p *PipeIndexProcessor) Dump() {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary

	// dump metadata
	metadata, err := json.Marshal(p.indexer.Metadata)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot dump metadata")
	}
	if err = ioutil.WriteFile(p.fMetadata(), metadata, 0644); err != nil {
		log.Fatal().Err(err).Msg("cannot dump metadata")
	}
	log.Info().Msgf("dump metadata to file=%s", p.fMetadata())

	// 分段dump dict
	for idx, shard := range p.indexer.Dict {
		pDict, err := json.Marshal(shard)
		if err != nil {
			log.Fatal().Err(err).Msgf("cannot dump %d-term-indexing", idx)
		}
		if err = ioutil.WriteFile(p.fPartialDict(idx), pDict, 0644); err != nil {
			log.Fatal().Err(err).Msgf("cannot dump %d-term-indexing", idx)
		}
		log.Info().Msgf("dump %d-term-indexing to file=%s", idx, p.fMetadata())
	}
}

// Load 从存储硬件加载索引结构.
func (p *PipeIndexProcessor) Load() {
	if utils.FileExist(p.cfg.DumpPath) {
		var json = jsoniter.ConfigCompatibleWithStandardLibrary

		// load metadata
		metadata, err := ioutil.ReadFile(p.fMetadata())
		if err != nil {
			log.Fatal().Err(err).Msg("cannot load metadata")
		}
		if err = json.Unmarshal(metadata, p.indexer.Metadata); err != nil {
			log.Fatal().Err(err).Msg("cannot load metadata")
		}
		log.Info().Msgf("load metadata from file=%s", p.fMetadata())

		// 分段load metadata
		for idx := range p.indexer.Dict {
			pDict, err := ioutil.ReadFile(p.fPartialDict(idx))
			if err != nil {
				log.Fatal().Err(err).Msgf("cannot load %d-term-indexing", idx)
			}
			if err = json.Unmarshal(pDict, p.indexer.Dict[idx]); err != nil {
				log.Fatal().Err(err).Msgf("cannot load %d-term-indexing", idx)
			}
			log.Info().Msgf("load %d-term-indexing from file=%s", idx, p.fMetadata())
		}
	}
}

func (p *PipeIndexProcessor) fMetadata() string {
	return filepath.Join(p.cfg.DumpPath, "metadata.json")
}

func (p *PipeIndexProcessor) fPartialDict(i int) string {
	return filepath.Join(p.cfg.DumpPath, fmt.Sprintf("term-indexing-%d.json", i))
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
	log.Info().Msg("start to build tf-idf ...")
	p.tfidf = &TFIDF{
		Vectors: make([]*DocVector, p.GetDoc()),
	}
	var i uint64
	for i = 0; i < p.GetDoc(); i++ {
		p.tfidf.Vectors[i] = &DocVector{
			Space: make([]float32, p.GetVocabulary()),
		}
	}
	D := p.GetDoc()
	for _, shard := range p.indexer.Dict {
		shard.mu.RLock()
		for _, pl := range shard.Backend {
			termIdx, _ := strconv.ParseUint(pl.TermID, 10, 64)
			for cur := pl.Postings.Next; cur != nil; cur = cur.Next {
				p.tfidf.Vectors[cur.DocIdx-1].DocID = cur.DocID
				p.tfidf.Vectors[cur.DocIdx-1].Space[termIdx-1] = float32(cur.TermFrequency) * float32(math.Log2(float64(D)/float64(pl.DocFrequency)))
			}
			if cur := pl.Postings.Next; cur != nil {
				if cur.TermFrequency > p.indexer.Metadata.MaxTermFrequency {
					p.indexer.Metadata.MaxTermFrequency = cur.TermFrequency
				}
			}
		}
		shard.mu.RUnlock()
	}
	log.Info().Msg("tf-idf has been builded")
}

// BuildQueryVector 构造查询向量.
func (p *PipeIndexProcessor) BuildQueryVector(concordance map[string]uint64) *QueryVector {
	q := &QueryVector{
		Space: make([]float32, p.GetVocabulary()),
	}
	D := p.GetDoc()
	for term, freq := range concordance {
		shard := p.indexer.Dict[fnv_1a_32(term)&0x1f]
		shard.mu.RLock()
		if pl, ok := shard.Backend[term]; ok {
			termIdx, _ := strconv.ParseUint(pl.TermID, 10, 64)
			q.Space[termIdx-1] = (0.5 + (0.5*float32(freq))/float32(p.indexer.Metadata.MaxTermFrequency)) * float32(math.Log2(float64(D)/float64(pl.DocFrequency)))
		}
		shard.mu.RUnlock()
	}
	return q
}

// TopK 计算查询向量与文档向量集合中各个向量的相似度，并返回最相似的k个文档
func (p *PipeIndexProcessor) TopK(k uint32, q *QueryVector) []string {
	ret := make([]string, 0, k)

	var qMagnitude float64
	for _, x := range q.Space {
		qMagnitude += float64(x) * float64(x)
	}
	qMagnitude = math.Sqrt(qMagnitude)
	if qMagnitude == 0.0 {
		return ret
	}

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
		if dMagnitude == 0.0 {
			continue
		}

		similarity = dot / (dMagnitude * qMagnitude)
		if similarity == 0.0 {
			continue
		}

		y := &SimilarObject{DocID: v.DocID, Similarity: similarity}
		if uint32(h.Len()) >= k {
			x := heap.Pop(h).(*SimilarObject)
			if x.Similarity < similarity {
				heap.Push(h, y)
			} else {
				heap.Push(h, x)
			}
		} else {
			heap.Push(h, y)
		}
	}

	for h.Len() > 0 {
		ret = append(ret, heap.Pop(h).(*SimilarObject).DocID)
	}

	return ret
}

// GetDocCapacity 返回文档总量上限.
func (p *PipeIndexProcessor) GetDocCapacity() uint64 {
	return _DocCapacity
}

// GetDoc 返回文档总量.
func (p *PipeIndexProcessor) GetDoc() uint64 {
	return atomic.LoadUint64(&(p.indexer.Metadata.Doc))
}

// GetVocabularyCapacity 返回词汇总量上限.
func (p *PipeIndexProcessor) GetVocabularyCapacity() uint64 {
	return _VocabularyCapacity
}

// GetVocabulary 返回词汇总量.
func (p *PipeIndexProcessor) GetVocabulary() uint64 {
	return atomic.LoadUint64(&(p.indexer.Metadata.Vocabulary))
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

func (m *DocStore) set(docID string) {
	m.mu.Lock()
	buf := make([]byte, 8)
	id, _ := strconv.ParseUint(docID, 10, 64)
	binary.BigEndian.PutUint64(buf, 1<<(id&_Mask))
	for i := 0; i < 8; i++ {
		m.BitSet[id>>_Shift][i] = m.BitSet[id>>_Shift][i] | buf[i]
	}
	m.mu.Unlock()
}

func (m *DocStore) clear(docID string) { // nolint
	m.mu.Lock()
	buf := make([]byte, 8)
	id, _ := strconv.ParseUint(docID, 10, 64)
	binary.BigEndian.PutUint64(buf, 1<<(id&_Mask))
	for i := 0; i < 8; i++ {
		m.BitSet[id>>_Shift][i] = m.BitSet[id>>_Shift][i] & ^(buf[i])
	}
	m.mu.Unlock()
}

func (m *DocStore) exist(docID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	buf := make([]byte, 8)
	id, _ := strconv.ParseUint(docID, 10, 64)
	binary.BigEndian.PutUint64(buf, 1<<(id&_Mask))
	var exist bool
	for i := 0; i < 8; i++ {
		exist = exist || (m.BitSet[id>>_Shift][i]&buf[i] != 0)
	}
	return exist
}

func (m *VocabularyStore) set(termID string) {
	m.mu.Lock()
	buf := make([]byte, 8)
	id, _ := strconv.ParseUint(termID, 10, 64)
	binary.BigEndian.PutUint64(buf, 1<<(id&_Mask))
	for i := 0; i < 8; i++ {
		m.BitSet[id>>_Shift][i] = m.BitSet[id>>_Shift][i] | buf[i]
	}
	m.mu.Unlock()
}

func (m *VocabularyStore) clear(termID string) { // nolint
	m.mu.Lock()
	buf := make([]byte, 8)
	id, _ := strconv.ParseUint(termID, 10, 64)
	binary.BigEndian.PutUint64(buf, 1<<(id&_Mask))
	for i := 0; i < 8; i++ {
		m.BitSet[id>>_Shift][i] = m.BitSet[id>>_Shift][i] & ^(buf[i])
	}
	m.mu.Unlock()
}

func (m *VocabularyStore) exist(termID string) bool { // nolint
	m.mu.RLock()
	defer m.mu.RUnlock()
	buf := make([]byte, 8)
	id, _ := strconv.ParseUint(termID, 10, 64)
	binary.BigEndian.PutUint64(buf, 1<<(id&_Mask))
	var exist bool
	for i := 0; i < 8; i++ {
		exist = exist || (m.BitSet[id>>_Shift][i]&buf[i] != 0)
	}
	return exist
}
