package stemming

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
)

func TestEnApplyStemming(t *testing.T) {
	inConcordance := map[string]uint64{
		"a":           1,
		"aaron":       1,
		"abaissiez":   1,
		"abandon":     1,
		"abandoned":   1,
		"abase":       1,
		"abash":       1,
		"abate":       1,
		"abated":      1,
		"abatement":   1,
		"abatements":  1,
		"abates":      1,
		"abbess":      1,
		"abbey":       1,
		"abbeys":      1,
		"abbominable": 1,
		"abbot":       1,
		"abbots":      1,
		"abbreviated": 1,
		"abed":        1,
	}
	ouConcordance := map[string]uint64{
		"a":         1,
		"aaron":     1,
		"ab":        1,
		"abaissiez": 1,
		"abandon":   2,
		"abas":      1,
		"abash":     1,
		"abat":      5,
		"abbess":    1,
		"abbei":     2,
		"abbomin":   1,
		"abbot":     2,
		"abbrevi":   1,
	}

	p := &PipeStemmingProcessor{
		tokenBucket: make(chan struct{}, 1),
	}
	inpacket := &common.ConcordanceWrapper{
		Concordance: inConcordance,
	}
	output := make(common.ConcordanceChannel)

	exit := make(chan struct{})

	go func() {
		outpacket := <-output
		for k, v := range ouConcordance {
			vv, ok := outpacket.Concordance[k]
			assert.Equal(t, ok, true)
			assert.Equal(t, vv, v)
		}
		exit <- struct{}{}
	}()

	p.applyEnglishStemming(inpacket, output)

	<-exit
}

func TestChApplyStemming(t *testing.T) {
}
