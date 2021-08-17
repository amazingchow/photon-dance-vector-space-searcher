package tokenize

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"unicode"

	"github.com/huichen/sego"
	"github.com/rs/zerolog/log"

	pb "github.com/amazingchow/photon-dance-vector-space-searcher/api"
	"github.com/amazingchow/photon-dance-vector-space-searcher/internal/common"
	"github.com/amazingchow/photon-dance-vector-space-searcher/internal/storage"
)

// PipeTokenizeProcessor 文本分词器
type PipeTokenizeProcessor struct {
	tokenBucket chan struct{}
	storage     storage.Persister
	language    common.LanguageType
	chSegmenter *sego.Segmenter
	chRegExp    *regexp.Regexp
}

// NewPipeTokenizeProcessor 新建文本分词器.
func NewPipeTokenizeProcessor(storage storage.Persister, language common.LanguageType) *PipeTokenizeProcessor {
	p := &PipeTokenizeProcessor{
		tokenBucket: make(chan struct{}, 20),
		storage:     storage,
		language:    language,
	}
	if language == common.LanguageTypeChinsese {
		p.chSegmenter = new(sego.Segmenter)
		p.chSegmenter.LoadDictionary("dict/dictionary.txt")
		p.chRegExp = regexp.MustCompile("[\u4E00-\u9FA5]+")
	}
	log.Info().Msg("load PipeTokenizeProcessor plugin")
	return p
}

// InfoTokenize 对中/英文文本进行分词.
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
				if packet.DeliveryStatus == pb.PacketDeliveryStatus_InDelivery {
					switch p.language {
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
		default:
			{

			}
		}
	}
	pGroup.Done()
	log.Info().Msg("unload PipeTokenizeProcessor plugin")
}

func (p *PipeTokenizeProcessor) tokenizeEnglishDoc(packet *pb.Packet, output common.ConcordanceChannel) {
	p.tokenBucket <- struct{}{}

	file := &common.File{
		Type: packet.DocType,
		Name: packet.DocId,
		Body: make([]string, 0),
	}
	if _, err := p.storage.Readable(context.Background(), file); err != nil {
		log.Error().Err(err)
		return
	}
	if _, err := p.storage.Get(context.Background(), file); err != nil {
		log.Error().Err(err)
		return
	}

	concordance := make(map[string]uint64)
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

	<-p.tokenBucket
}

func (p *PipeTokenizeProcessor) tokenizeChineseDoc(packet *pb.Packet, output common.ConcordanceChannel) {
	p.tokenBucket <- struct{}{}

	file := &common.File{
		Type: packet.DocType,
		Name: packet.DocId,
		Body: make([]string, 0),
	}
	if _, err := p.storage.Readable(context.Background(), file); err != nil {
		log.Error().Err(err)
		return
	}
	if _, err := p.storage.Get(context.Background(), file); err != nil {
		log.Error().Err(err)
		return
	}

	concordance := make(map[string]uint64)
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
		segments := p.chSegmenter.Segment([]byte(line))
		words := p.chRegExp.FindAllString(sego.SegmentsToString(segments, false), -1)
		wordsCh <- common.WordsWrapper{Words: words}
	}
	close(wordsCh)

	<-exit

	output <- &common.ConcordanceWrapper{
		DocID:       packet.DocId,
		Concordance: concordance,
	}
	log.Debug().Msg("PipeTokenizeProcessor processes one data packet")

	<-p.tokenBucket
}

// QueryTokenize 对查询语句进行分词.
func (p *PipeTokenizeProcessor) QueryTokenize(query string, language common.LanguageType, concordance map[string]uint64) {
	p.tokenBucket <- struct{}{}

	if language == common.LanguageTypeEnglish {
		fc := func(r rune) bool { return !unicode.IsLetter(r) }
		words := strings.FieldsFunc(query, fc)
		for _, w := range words {
			concordance[strings.ToLower(w)]++
		}
	} else if language == common.LanguageTypeChinsese {
		segments := p.chSegmenter.Segment([]byte(query))
		words := p.chRegExp.FindAllString(sego.SegmentsToString(segments, false), -1)
		for _, w := range words {
			concordance[w]++
		}
	}

	<-p.tokenBucket
}
