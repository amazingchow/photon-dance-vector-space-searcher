package parse

import (
	"os"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/rs/zerolog/log"

	pb "github.com/amazingchow/engine-vector-space-search-service/api"
	"github.com/amazingchow/engine-vector-space-search-service/internal/common"
	"github.com/amazingchow/engine-vector-space-search-service/internal/storage"
)

// PipeParseProcessor 文本解析器.
type PipeParseProcessor struct {
	tokenBucket chan struct{}
	storage     storage.Persister
}

// NewPipeParseProcessor 新建文本解析器.
func NewPipeParseProcessor(storage storage.Persister) *PipeParseProcessor {
	log.Info().Msg("load PipeParseProcessor plugin")
	return &PipeParseProcessor{
		tokenBucket: make(chan struct{}, 20),
		storage:     storage,
	}
}

// InfoExtract 解析中/英文本文件.
func (p *PipeParseProcessor) InfoExtract(pGroup *sync.WaitGroup, input common.PacketChannel, output common.PacketChannel) {
	pGroup.Add(1)
LOOP_LABEL:
	for {
		select {
		case packet, ok := <-input:
			{
				if !ok {
					close(output)
					break LOOP_LABEL
				}
				if packet.DeliveryStatus == pb.PacketDeliveryStatus_InDelivery {
					switch packet.WebStation {
					case pb.WebStation_MOFRPC:
						{
							go p.parseMOFRPCHTML(packet, output)
						}
					default:
						{

						}
					}
				}
			}
		}
	}
	pGroup.Done()
	log.Info().Msg("unload PipeParseProcessor plugin")
}

// 用于解析中华人民共和国财政部发布的文章网页
func (p *PipeParseProcessor) parseMOFRPCHTML(packet *pb.Packet, output common.PacketChannel) {
	p.tokenBucket <- struct{}{}

	path, err := p.storage.Readable(&common.File{
		Type: packet.DocType,
		Name: packet.DocId,
	})
	if err != nil {
		log.Error().Err(err)
		return
	}

	fr, err := os.Open(path)
	if err != nil {
		log.Error().Err(err)
		return
	}
	defer fr.Close()

	doc, err := goquery.NewDocumentFromReader(fr)
	if err != nil {
		log.Error().Err(err)
		return
	}

	body := make([]string, 0)
	doc.Find("div.my_conboxzw div.TRS_Editor div.TRS_Editor p").Each(func(i int, s *goquery.Selection) {
		body = append(body, strings.TrimSpace(s.Text()))
	})

	if _, err = p.storage.Writable(&common.File{
		Type: pb.DocType_TextDoc,
		Name: packet.DocId,
		Body: body,
	}); err != nil {
		log.Error().Err(err)
		return
	}
	if _, err = p.storage.Put(&common.File{
		Type: pb.DocType_TextDoc,
		Name: packet.DocId,
	}); err != nil {
		log.Error().Err(err)
		return
	}

	output <- &pb.Packet{
		DocType: pb.DocType_TextDoc,
		DocId:   packet.DocId,
	}
	log.Debug().Msg("PipeParseProcessor processes one data packet")

	<-p.tokenBucket
}
