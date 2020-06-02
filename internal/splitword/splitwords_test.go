package splitword

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitWords(t *testing.T) {
	w, err := SplitWords("fixtures/input.text")
	assert.Empty(t, err)
	t.Logf("gutenberg: %d", w["gutenberg"])
}
