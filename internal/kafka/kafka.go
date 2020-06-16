package kafka

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rs/zerolog/log"

	conf "github.com/amazingchow/engine-vector-space-search-service/internal/config"
)

// ConsumerHandler 统一处理kafka数据.
type ConsumerHandler struct {
	once     sync.Once
	cfg      *conf.KafkaConfig
	msgCh    chan *sarama.ConsumerMessage
	readyCh  chan struct{}
	consumer sarama.ConsumerGroup
	closer   func()
}

// Msg 返回消费通道.
func (h *ConsumerHandler) Msg() <-chan *sarama.ConsumerMessage {
	return h.msgCh
}

// Setup sarama.ConsumerGroupHandler接口定义实现.
func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.readyCh)
	return nil
}

// Cleanup sarama.ConsumerGroupHandler接口定义实现.
func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// Close 关闭并释放所有资源 (并发安全).
func (h *ConsumerHandler) Close() {
	h.once.Do(func() { h.closer() })
}

// ConsumeClaim sarama.ConsumerGroupHandler接口定义实现.
func (h *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			{
				log.Info().Msg("consumer claim context done")
				return nil
			}
		case msg, ok := <-claim.Messages():
			{
				if !ok {
					log.Warn().Msg("consumer claim channel closed")
					return nil
				}
				h.msgCh <- msg
				session.MarkMessage(msg, "")
			}
		}
	}
}

// NewConsumerHandler 新建消费者.
func NewConsumerHandler(cfg *conf.KafkaConfig) (*ConsumerHandler, error) {
	h := &ConsumerHandler{
		cfg:     cfg,
		msgCh:   make(chan *sarama.ConsumerMessage),
		readyCh: make(chan struct{}),
	}

	var err error
	sc := sarama.NewConfig()
	sc.Version, err = sarama.ParseKafkaVersion(h.cfg.Version)
	if err != nil {
		log.Error().Err(err).Msgf("wrong kafka version %s", h.cfg.Version)
		return nil, err
	}
	setKafkaAccessSettings(sc)

	h.consumer, err = sarama.NewConsumerGroup(h.cfg.Brokers, h.cfg.ConsumeGroup, sc)
	if err != nil {
		log.Error().Err(err).Msgf("cannot create consumer group for brokers <%v>", h.cfg.Brokers)
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	kGroup := new(sync.WaitGroup)
	kGroup.Add(1)
	go func() {
		h.start(ctx, h.cfg.Topic)
		kGroup.Done()
	}()

	<-h.readyCh

	h.closer = func() {
		cancel()
		h.consumer.Close()
		close(h.msgCh)
		kGroup.Wait()
	}

	return h, nil
}

func (h *ConsumerHandler) start(ctx context.Context, topics []string) {
	for {
		if err := h.consumer.Consume(ctx, topics, h); err != nil {
			log.Error().Err(err).Msg("error occurs inside consumer")
		}

		select {
		case <-ctx.Done():
			{
				log.Info().Msg("consumer group context done ...")
				return
			}
		case err := <-h.consumer.Errors():
			{
				log.Error().Err(err).Msg("error occurs inside consumer")
			}
		default:
			{
				log.Info().Msg("rebalance after 2s ...")
				time.Sleep(time.Second * 2) // 防止刷屏
			}
		}

		h.readyCh = make(chan struct{})
	}
}

func setKafkaAccessSettings(cfg *sarama.Config) {
	usr := os.Getenv("KAFKA_USERNAME")
	pwd := os.Getenv("KAFKA_PASSWORD")
	if usr == "" || pwd == "" {
		log.Info().Msg("access kafka without SASL setting")
		return
	}
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	cfg.Net.SASL.User = usr
	cfg.Net.SASL.Password = pwd
	cfg.Net.SASL.Version = sarama.SASLHandshakeV1
}
