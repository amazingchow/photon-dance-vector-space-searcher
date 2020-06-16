package stemming

import (
	stemmer "github.com/agonopol/go-stem"
)

/*
	https://tartarus.org/martin/PorterStemmer/index.html
*/

// PipeStemmingProcessor 词干抽取器
type PipeStemmingProcessor struct{}

// NewPipeStemmingProcessor 新建词干抽取器.
func NewPipeStemmingProcessor() *PipeStemmingProcessor {
	return &PipeStemmingProcessor{}
}

// EnApplyStemming 抽取英文词汇的词干, 并重构concordance.
func (p *PipeStemmingProcessor) EnApplyStemming(concordance map[string]uint32) {
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
}

// ChApplyStemming 抽取中文词汇的词干, 并重构concordance.
func (p *PipeStemmingProcessor) ChApplyStemming(concordance map[string]uint32) {
	// TODO
}
