package stopword

// PipeStopWordsProcessor 停词器
type PipeStopWordsProcessor struct{}

// NewPipeStopWordsProcessor 新建停词器.
func NewPipeStopWordsProcessor() *PipeStopWordsProcessor {
	return &PipeStopWordsProcessor{}
}

// EnRemoveStopWords 移除concordance中的英文+特殊停词.
func (p *PipeStopWordsProcessor) EnRemoveStopWords(concordance map[string]uint32) {
	for k := range concordance {
		if _, ok := EnStopWords[k]; ok {
			delete(concordance, k)
		} else if _, ok := SpStopWords[k]; ok {
			delete(concordance, k)
		}
	}
}

// ChRemoveStopWords 移除concordance中的中文+特殊停词.
func (p *PipeStopWordsProcessor) ChRemoveStopWords(concordance map[string]uint32) {
	for k := range concordance {
		if _, ok := ChStopWords[k]; ok {
			delete(concordance, k)
		} else if _, ok := SpStopWords[k]; ok {
			delete(concordance, k)
		}
	}
}
