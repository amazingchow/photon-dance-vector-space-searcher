package stopword

import (
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
)

// PipeStopWordsProcessor 停词器
type PipeStopWordsProcessor struct {
	TokenBucket chan struct{}
	Language    common.LanguageType
}

// NewPipeStopWordsProcessor 新建停词器.
func NewPipeStopWordsProcessor(language common.LanguageType) *PipeStopWordsProcessor {
	log.Info().Msg("load PipeStopWordsProcessor plugin")
	return &PipeStopWordsProcessor{
		TokenBucket: make(chan struct{}, 20),
		Language:    language,
	}
}

// RemoveStopWords 移除concordance中的中/英文停词+特殊停词.
func (p *PipeStopWordsProcessor) RemoveStopWords(pGroup *sync.WaitGroup, input common.ConcordanceChannel, output common.ConcordanceChannel) {
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
						go p.removeEnglishStopWords(packet, output)
					}
				case common.LanguageTypeChinsese:
					{
						go p.removeChineseStopWords(packet, output)
					}
				default:
					{

					}
				}
			}
		}
	}
	pGroup.Done()
	log.Info().Msg("unload PipeStopWordsProcessor plugin")
}

func (p *PipeStopWordsProcessor) removeEnglishStopWords(packet *common.ConcordanceWrapper, output common.ConcordanceChannel) {
	p.TokenBucket <- struct{}{}

	for k := range packet.Concordance {
		if _, ok := EnStopWords[k]; ok {
			delete(packet.Concordance, k)
		} else if _, ok := SpStopWords[k]; ok {
			delete(packet.Concordance, k)
		}
	}

	output <- &common.ConcordanceWrapper{
		DocID:       packet.DocID,
		Concordance: packet.Concordance,
	}
	log.Debug().Msg("PipeStopWordsProcessor processes one data packet")

	<-p.TokenBucket
}

func (p *PipeStopWordsProcessor) removeChineseStopWords(packet *common.ConcordanceWrapper, output common.ConcordanceChannel) {
	p.TokenBucket <- struct{}{}

	for k := range packet.Concordance {
		if _, ok := ChStopWords[k]; ok {
			delete(packet.Concordance, k)
		} else if _, ok := SpStopWords[k]; ok {
			delete(packet.Concordance, k)
		}
	}

	output <- &common.ConcordanceWrapper{
		DocID:       packet.DocID,
		Concordance: packet.Concordance,
	}
	log.Debug().Msg("PipeStopWordsProcessor processes one data packet")

	<-p.TokenBucket
}
