package parse

import (
	"log"
	"os"
	"strings"

	"github.com/PuerkitoBio/goquery"

	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
	"github.com/amazingchow/engine-vector-space-search-service/internal/storage"
)

// PipeParseProcessor 文本解析器.
type PipeParseProcessor struct {
	TokenBucket chan struct{}
	Storage     storage.Persister
}

// NewPipeParseProcessor 新建文本解析器.
func NewPipeParseProcessor(storage storage.Persister) *PipeParseProcessor {
	return &PipeParseProcessor{
		TokenBucket: make(chan struct{}, 20),
		Storage:     storage,
	}
}

// ExtractInfo 解析文本文件.
func (p *PipeParseProcessor) ExtractInfo(input common.Pipeline, output common.Pipeline) {
LOOP_LABEL:
	for {
		select {
		case packet, ok := <-input:
			{
				if !ok {
					close(output)
					break LOOP_LABEL
				}
				switch packet.WebStation {
				case common.MOFRPC:
					{
						go p.parseMOFRPCHTMLText(packet, output)
					}
				default:
					{

					}
				}
			}
		}
	}
}

// 用于解析中华人民共和国财政部发布的文章网页
func (p *PipeParseProcessor) parseMOFRPCHTMLText(packet *common.Packet, output common.Pipeline) {
	p.TokenBucket <- struct{}{}

	path, err := p.Storage.Readable(&common.File{
		Type: packet.FileType,
		Name: packet.FileTitle,
	})
	if err != nil {
		log.Fatal(err)
	}

	fr, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer fr.Close()

	doc, err := goquery.NewDocumentFromReader(fr)
	if err != nil {
		log.Fatal(err)
	}

	body := make([]string, 0)
	doc.Find("div.my_conboxzw div.TRS_Editor div.TRS_Editor p").Each(func(i int, s *goquery.Selection) {
		body = append(body, strings.TrimSpace(s.Text()))
	})

	_, err = p.Storage.Writable(&common.File{
		Type: common.TextFile,
		Name: packet.FileTitle,
		Body: body,
	})
	if err != nil {
		log.Fatal(err)
	}

	output <- &common.Packet{
		FileType:  common.TextFile,
		FileTitle: packet.FileTitle,
	}

	<-p.TokenBucket
}
