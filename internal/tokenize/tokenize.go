package tokenize

import (
	"log"
	"regexp"
	"strings"
	"unicode"

	"github.com/huichen/sego"

	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
	"github.com/amazingchow/engine-vector-space-search-service/internal/storage"
)

// PipeTokenizeProcessor 分词器
type PipeTokenizeProcessor struct {
	Storage storage.Persister
}

type wordsWrapper struct {
	words []string
}

// EnTokenize 对英文文本进行分词, 输出一个concordance.
func (p *PipeTokenizeProcessor) EnTokenize(input common.Pipeline) (map[string]uint32, error) {
LOOP_LABEL:
	for {
		select {
		case packet, ok := <-input:
			{
				if !ok {
					break LOOP_LABEL
				}

				file := &common.File{
					Type: packet.FileType,
					Name: packet.FileTitle,
					Body: make([]string, 0),
				}
				_, err := p.Storage.Readable(file)
				if err != nil {
					log.Fatal(err)
				}
				_, err = p.Storage.Get(file)
				if err != nil {
					log.Fatal(err)
				}

				concordance := make(map[string]uint32)

				exit := make(chan struct{})

				wordsCh := make(chan wordsWrapper, 10)
				go func() {
					for x := range wordsCh {
						for _, w := range x.words {
							concordance[strings.ToLower(w)]++
						}
					}
					exit <- struct{}{}
				}()

				fc := func(r rune) bool { return !unicode.IsLetter(r) }
				for _, line := range file.Body {
					words := strings.FieldsFunc(line, fc)
					wordsCh <- wordsWrapper{words: words}
				}
				close(wordsCh)

				<-exit

				return concordance, nil
			}
		}
	}

	return nil, nil
}

var (
	_ChSegmenter sego.Segmenter
	_ChRegExp    = regexp.MustCompile("[\u4E00-\u9FA5]+")
)

// ChTokenize 对中文文本进行分词, 输出一个concordance.
func (p *PipeTokenizeProcessor) ChTokenize(input common.Pipeline) (map[string]uint32, error) {
LOOP_LABEL:
	for {
		select {
		case packet, ok := <-input:
			{
				if !ok {
					break LOOP_LABEL
				}

				file := &common.File{
					Type: packet.FileType,
					Name: packet.FileTitle,
					Body: make([]string, 0),
				}
				_, err := p.Storage.Readable(file)
				if err != nil {
					log.Fatal(err)
				}
				_, err = p.Storage.Get(file)
				if err != nil {
					log.Fatal(err)
				}

				concordance := make(map[string]uint32)

				exit := make(chan struct{})

				wordsCh := make(chan wordsWrapper, 10)
				go func() {
					for x := range wordsCh {
						for _, w := range x.words {
							concordance[w]++
						}
					}
					exit <- struct{}{}
				}()

				for _, line := range file.Body {
					segments := _ChSegmenter.Segment([]byte(line))
					words := _ChRegExp.FindAllString(sego.SegmentsToString(segments, false), -1)
					wordsCh <- wordsWrapper{words: words}
				}
				close(wordsCh)

				<-exit

				return concordance, nil
			}
		}
	}

	return nil, nil
}

func init() {
	_ChSegmenter.LoadDictionary("./dict/dictionary.txt")
}
