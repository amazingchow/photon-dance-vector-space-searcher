package splitword

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitEnWords(t *testing.T) {
	w, err := SplitEnWords("fixtures/input_en.text")
	assert.Empty(t, err)
	t.Logf("gutenberg: %d", w["gutenberg"])
}
