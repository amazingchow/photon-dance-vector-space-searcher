package splitword

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitEnWords(t *testing.T) {
	w, err := SplitEnWords("fixtures/input_en.txt")
	assert.Empty(t, err)
	t.Logf("gutenberg: %d", w["gutenberg"])
}

func TestSplitChWords(t *testing.T) {
	w, err := SplitChWords("fixtures/input_ch.txt")
	assert.Empty(t, err)
	t.Logf("减税: %d", w["减税"])
}
