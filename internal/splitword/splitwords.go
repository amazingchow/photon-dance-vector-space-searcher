package splitword

import (
	"bufio"
	"os"
	"strings"
	"unicode"

	"github.com/amazingchow/engine-vector-space-search-service/internal/stopword"
)

type wordsWrapper struct {
	words []string
}

// SplitWords splits input file's text content into word counter.
func SplitWords(fn string) (map[string]uint32, error) {
	wordCounter := make(map[string]uint32)

	fd, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	exit := make(chan struct{})

	wordsCh := make(chan wordsWrapper, 10)
	go func() {
		for x := range wordsCh {
			for _, w := range x.words {
				if _, ok := stopword.EnStopWords[strings.ToLower(w)]; !ok {
					wordCounter[strings.ToLower(w)]++
				}
			}
		}
		exit <- struct{}{}
	}()

	scanner := bufio.NewScanner(fd)
	fc := func(r rune) bool { return !unicode.IsLetter(r) }
	for scanner.Scan() {
		words := strings.FieldsFunc(scanner.Text(), fc)
		wordsCh <- wordsWrapper{words: words}
	}
	close(wordsCh)

	err = scanner.Err()

	<-exit

	return wordCounter, err
}
