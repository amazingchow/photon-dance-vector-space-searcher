package stemming

import (
	"fmt"
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
	TokenBucket chan struct{}
	Language    common.LanguageType
}

// NewPipeStemmingProcessor 新建词干抽取器.
func NewPipeStemmingProcessor(language common.LanguageType) *PipeStemmingProcessor {
	log.Info().Msg("load PipeStemmingProcessor plugin")
	return &PipeStemmingProcessor{
		TokenBucket: make(chan struct{}, 20),
		Language:    language,
	}
}

// ApplyStemming 抽取中/英文词汇的词干, 并重构concordance.
func (p *PipeStemmingProcessor) ApplyStemming(pGroup *sync.WaitGroup, input common.ConcordanceChannel, output common.ConcordanceChannel) {
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

func (p *PipeStemmingProcessor) applyEnglishStemming(packet *common.ConcordanceWrapper, output common.ConcordanceChannel) {
	p.TokenBucket <- struct{}{}

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

	// output <- &common.ConcordanceWrapper{
	// 	Concordance: packet.Concordance,
	// }
	log.Debug().Msg("PipeStemmingProcessor processes one data packet")

	<-p.TokenBucket
}

func (p *PipeStemmingProcessor) applyChineseStemming(packet *common.ConcordanceWrapper, output common.ConcordanceChannel) {
	p.TokenBucket <- struct{}{}

	// 词干提取是英文语料预处理的一个步骤, 中文并不需要

	fmt.Printf("%v\n", packet.Concordance)

	// output <- &common.ConcordanceWrapper{
	// 	Concordance: packet.Concordance,
	// }
	log.Debug().Msg("PipeStemmingProcessor processes one data packet")

	<-p.TokenBucket
}
