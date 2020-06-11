package stopword

// EnRemoveStopWords remove English stopwords inside the concordance.
func EnRemoveStopWords(concordance map[string]uint32) {
	for k := range concordance {
		if _, ok := EnStopWords[k]; ok {
			delete(concordance, k)
		} else if _, ok := SpStopWords[k]; ok {
			delete(concordance, k)
		}
	}
}

// ChRemoveStopWords remove Chinese stopwords inside the concordance.
func ChRemoveStopWords(concordance map[string]uint32) {
	for k := range concordance {
		if _, ok := ChStopWords[k]; ok {
			delete(concordance, k)
		} else if _, ok := SpStopWords[k]; ok {
			delete(concordance, k)
		}
	}
}
