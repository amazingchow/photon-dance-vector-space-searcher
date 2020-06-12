package parse

import (
	"log"
	"os"

	"github.com/PuerkitoBio/goquery"

	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
)

var _TokenBucket chan struct{}

// ExtractInfo parses input raw text.
func ExtractInfo(input common.Pipeline, output common.Pipeline) {
LOOP_LABEL:
	for {
		select {
		case p, ok := <-input:
			{
				if !ok {
					close(output)
					break LOOP_LABEL
				}
				switch p.WebStation {
				case common.MOFRPC:
					{
						go parseMOFRPCHTMLText(p, output)
					}
				default:
					{

					}
				}
			}
		default:
			{

			}
		}
	}
}

// 用于解析中华人民共和国财政部发布的文章网页
func parseMOFRPCHTMLText(p *common.Packet, output common.Pipeline) {
	_TokenBucket <- struct{}{}

	var fr *os.File
	if p.Local {
		fr, err := os.Open(p.Location)
		if err != nil {
			log.Fatal(err)
		}
		defer fr.Close()
	}

	doc, err := goquery.NewDocumentFromReader(fr)
	if err != nil {
		log.Fatal(err)
	}

	// Find the review items
	doc.Find("div.my_conboxzw div.TRS_Editor div.TRS_Editor p").Each(func(i int, s *goquery.Selection) {
		// line := strings.TrimSpace(s.Text())
	})

	<-_TokenBucket
}

func init() {
	_TokenBucket = make(chan struct{}, 20)
}
