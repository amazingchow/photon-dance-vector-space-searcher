package tokenize

import (
	"regexp"
	"strings"
	"sync"
	"unicode"

	"github.com/huichen/sego"
	"github.com/rs/zerolog/log"

	pb "github.com/amazingchow/engine-vector-space-search-service/api"
	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
	"github.com/amazingchow/engine-vector-space-search-service/internal/storage"
)

// PipeTokenizeProcessor 文本分词器
type PipeTokenizeProcessor struct {
	TokenBucket chan struct{}
	Storage     storage.Persister
	Language    common.LanguageType
	ChSegmenter *sego.Segmenter
	ChRegExp    *regexp.Regexp
}

// NewPipeTokenizeProcessor 新建文本分词器.
func NewPipeTokenizeProcessor(storage storage.Persister, language common.LanguageType) *PipeTokenizeProcessor {
	p := &PipeTokenizeProcessor{
		TokenBucket: make(chan struct{}, 20),
		Storage:     storage,
		Language:    language,
	}
	if language == common.LanguageTypeChinsese {
		p.ChSegmenter = new(sego.Segmenter)
		p.ChSegmenter.LoadDictionary("dict/dictionary.txt")
		p.ChRegExp = regexp.MustCompile("[\u4E00-\u9FA5]+")
	}
	log.Info().Msg("load PipeTokenizeProcessor plugin")
	return p
}

// InfoTokenize 对中/英文本进行分词.
func (p *PipeTokenizeProcessor) InfoTokenize(pGroup *sync.WaitGroup, input common.PacketChannel, output common.ConcordanceChannel) {
	pGroup.Add(1)
LOOP_LABEL:
	for {
		select {
		case packet, ok := <-input:
			{
				if !ok {
					close(output)
					break LOOP_LABEL
				}
				switch p.Language {
				case common.LanguageTypeEnglish:
					{
						go p.tokenizeEnglishDoc(packet, output)
					}
				case common.LanguageTypeChinsese:
					{
						go p.tokenizeChineseDoc(packet, output)
					}
				default:
					{

					}
				}
			}
		}
	}
	pGroup.Done()
	log.Info().Msg("unload PipeTokenizeProcessor plugin")
}

func (p *PipeTokenizeProcessor) tokenizeEnglishDoc(packet *pb.Packet, output common.ConcordanceChannel) {
	p.TokenBucket <- struct{}{}

	file := &common.File{
		Type: packet.DocType,
		Name: packet.DocId,
		Body: make([]string, 0),
	}
	if _, err := p.Storage.Readable(file); err != nil {
		log.Error().Err(err)
		return
	}
	if _, err := p.Storage.Get(file); err != nil {
		log.Error().Err(err)
		return
	}

	concordance := make(map[string]uint32)
	wordsCh := make(chan common.WordsWrapper, 10)
	exit := make(chan struct{})

	go func() {
		for x := range wordsCh {
			for _, w := range x.Words {
				concordance[strings.ToLower(w)]++
			}
		}
		exit <- struct{}{}
	}()

	fc := func(r rune) bool { return !unicode.IsLetter(r) }
	for _, line := range file.Body {
		words := strings.FieldsFunc(line, fc)
		wordsCh <- common.WordsWrapper{Words: words}
	}
	close(wordsCh)

	<-exit

	output <- &common.ConcordanceWrapper{
		DocID:       packet.DocId,
		Concordance: concordance,
	}
	log.Debug().Msg("PipeTokenizeProcessor processes one data packet")

	<-p.TokenBucket
}

func (p *PipeTokenizeProcessor) tokenizeChineseDoc(packet *pb.Packet, output common.ConcordanceChannel) {
	p.TokenBucket <- struct{}{}

	file := &common.File{
		Type: packet.DocType,
		Name: packet.DocId,
		Body: make([]string, 0),
	}
	if _, err := p.Storage.Readable(file); err != nil {
		log.Error().Err(err)
		return
	}
	if _, err := p.Storage.Get(file); err != nil {
		log.Error().Err(err)
		return
	}

	concordance := make(map[string]uint32)
	wordsCh := make(chan common.WordsWrapper, 10)
	exit := make(chan struct{})

	go func() {
		for x := range wordsCh {
			for _, w := range x.Words {
				concordance[w]++
			}
		}
		exit <- struct{}{}
	}()

	for _, line := range file.Body {
		segments := p.ChSegmenter.Segment([]byte(line))
		words := p.ChRegExp.FindAllString(sego.SegmentsToString(segments, false), -1)
		wordsCh <- common.WordsWrapper{Words: words}
	}
	close(wordsCh)

	<-exit

	output <- &common.ConcordanceWrapper{
		DocID:       packet.DocId,
		Concordance: concordance,
	}
	log.Debug().Msg("PipeTokenizeProcessor processes one data packet")

	<-p.TokenBucket
}
