package stemming

import (
	stemmer "github.com/agonopol/go-stem"
)

/*
	https://tartarus.org/martin/PorterStemmer/index.html
*/

// EnApplyStemming applies stemming for English terms inside the concordance.
func EnApplyStemming(concordance map[string]uint32) {
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

// ChApplyStemming applies stemming for Chinese terms inside the concordance.
func ChApplyStemming(concordance map[string]uint32) {
	// TODO
}
