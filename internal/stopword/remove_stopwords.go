package stopword

import (
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
)

// PipeStopWordsProcessor 停词器
type PipeStopWordsProcessor struct {
	tokenBucket chan struct{}
	language    common.LanguageType
}

// NewPipeStopWordsProcessor 新建停词器.
func NewPipeStopWordsProcessor(language common.LanguageType) *PipeStopWordsProcessor {
	log.Info().Msg("load PipeStopWordsProcessor plugin")
	return &PipeStopWordsProcessor{
		tokenBucket: make(chan struct{}, 20),
		language:    language,
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
				switch p.language {
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

// QueryRemoveStopWords 移除查询语句中的停词.
func (p *PipeStopWordsProcessor) QueryRemoveStopWords(language common.LanguageType, concordance map[string]uint64) {
	p.tokenBucket <- struct{}{}

	if language == common.LanguageTypeEnglish {
		for k := range concordance {
			if _, ok := EnStopWords[k]; ok {
				delete(concordance, k)
			} else if _, ok := SpStopWords[k]; ok {
				delete(concordance, k)
			}
		}
	} else if language == common.LanguageTypeChinsese {
		for k := range concordance {
			if _, ok := ChStopWords[k]; ok {
				delete(concordance, k)
			} else if _, ok := SpStopWords[k]; ok {
				delete(concordance, k)
			}
		}
	}

	<-p.tokenBucket
}

func (p *PipeStopWordsProcessor) removeEnglishStopWords(packet *common.ConcordanceWrapper, output common.ConcordanceChannel) {
	p.tokenBucket <- struct{}{}

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

	<-p.tokenBucket
}

func (p *PipeStopWordsProcessor) removeChineseStopWords(packet *common.ConcordanceWrapper, output common.ConcordanceChannel) {
	p.tokenBucket <- struct{}{}

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

	<-p.tokenBucket
}
