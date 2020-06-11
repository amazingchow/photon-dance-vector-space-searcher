package tokenize

import (
	"bufio"
	"os"
	"regexp"
	"strings"
	"unicode"

	"github.com/huichen/sego"

	"github.com/amazingchow/engine-vector-space-search-service/internal/stopword"
)

type wordsWrapper struct {
	words []string
}

// EnTokenize splits input English text into concordance.
// A concordance is a counter of every word that occurs in the document.
func EnTokenize(fn string) (map[string]uint32, error) {
	concordance := make(map[string]uint32)

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
					concordance[strings.ToLower(w)]++
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

	return concordance, err
}

var (
	_ChSegmenter sego.Segmenter
	_ChRegExp    = regexp.MustCompile("[\u4E00-\u9FA5]+")
)

// ChTokenize splits input Chinese text into concordance.
// A concordance is a counter of every word that occurs in the document.
func ChTokenize(fn string) (map[string]uint32, error) {
	concordance := make(map[string]uint32)

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
				if _, ok := stopword.ChStopWords[w]; !ok {
					concordance[w]++
				}
			}
		}
		exit <- struct{}{}
	}()

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		segments := _ChSegmenter.Segment([]byte(scanner.Text()))
		words := _ChRegExp.FindAllString(sego.SegmentsToString(segments, false), -1)
		wordsCh <- wordsWrapper{words: words}
	}
	close(wordsCh)

	err = scanner.Err()

	<-exit

	return concordance, err
}

func init() {
	_ChSegmenter.LoadDictionary("./dict/dictionary.txt")
}
