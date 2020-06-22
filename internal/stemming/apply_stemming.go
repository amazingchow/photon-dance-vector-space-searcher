package stemming

import (
	"sync"

	stemmer "github.com/agonopol/go-stem"
	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
	"github.com/rs/zerolog/log"
)

/*
	https://tartarus.org/martin/PorterStemmer/index.html
*/

// PipeStemmingProcessor 词干抽取器
type PipeStemmingProcessor struct {
	tokenBucket chan struct{}
	language    common.LanguageType
}

// NewPipeStemmingProcessor 新建词干抽取器.
func NewPipeStemmingProcessor(language common.LanguageType) *PipeStemmingProcessor {
	log.Info().Msg("load PipeStemmingProcessor plugin")
	return &PipeStemmingProcessor{
		tokenBucket: make(chan struct{}, 20),
		language:    language,
	}
}

// ApplyStemming 抽取中/英文词汇的词干, 并重构concordance.
func (p *PipeStemmingProcessor) ApplyStemming(pGroup *sync.WaitGroup, input common.ConcordanceChannel, output common.ConcordanceChannel) {
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
						go p.applyEnglishStemming(packet, output)
					}
				case common.LanguageTypeChinsese:
					{
						go p.applyChineseStemming(packet, output)
					}
				default:
					{

					}
				}
			}
		}
	}
	pGroup.Done()
	log.Info().Msg("unload PipeStemmingProcessor plugin")
}

// QueryApplyStemming 抽取查询语句中的词干.
func (p *PipeStemmingProcessor) QueryApplyStemming(language common.LanguageType, concordance map[string]uint64) {
	p.tokenBucket <- struct{}{}

	if language == common.LanguageTypeEnglish {
		for k, v := range concordance {
			out := string(stemmer.Stem([]byte(k)))
			if vv, ok := concordance[out]; ok {
				concordance[out] = vv + v
				concordance[k] -= v
			} else {
				concordance[out] = v
			}
		}
		for k, v := range concordance {
			if v == 0 {
				delete(concordance, k)
			}
		}
	} else if language == common.LanguageTypeChinsese {
		// 词干提取是英文语料预处理的一个步骤, 中文并不需要
	}

	<-p.tokenBucket
}

func (p *PipeStemmingProcessor) applyEnglishStemming(packet *common.ConcordanceWrapper, output common.ConcordanceChannel) {
	p.tokenBucket <- struct{}{}

	for k, v := range packet.Concordance {
		out := string(stemmer.Stem([]byte(k)))
		if vv, ok := packet.Concordance[out]; ok {
			packet.Concordance[out] = vv + v
			packet.Concordance[k] -= v
		} else {
			packet.Concordance[out] = v
		}
	}
	for k, v := range packet.Concordance {
		if v == 0 {
			delete(packet.Concordance, k)
		}
	}

	output <- &common.ConcordanceWrapper{
		DocID:       packet.DocID,
		Concordance: packet.Concordance,
	}
	log.Debug().Msg("PipeStemmingProcessor processes one data packet")

	<-p.tokenBucket
}

func (p *PipeStemmingProcessor) applyChineseStemming(packet *common.ConcordanceWrapper, output common.ConcordanceChannel) {
	p.tokenBucket <- struct{}{}

	// 词干提取是英文语料预处理的一个步骤, 中文并不需要

	output <- &common.ConcordanceWrapper{
		DocID:       packet.DocID,
		Concordance: packet.Concordance,
	}
	log.Debug().Msg("PipeStemmingProcessor processes one data packet")

	<-p.tokenBucket
}
