package tokenize

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnTokenize(t *testing.T) {
	w, err := EnTokenize("fixtures/input_en.txt")
	assert.Empty(t, err)
	t.Logf("gutenberg: %d", w["gutenberg"])
}

func TestChTokenize(t *testing.T) {
	w, err := ChTokenize("fixtures/input_ch.txt")
	assert.Empty(t, err)
	t.Logf("减税: %d", w["减税"])
}
