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

// CustomConsumerGroupHandler 自定义消费句柄
type CustomConsumerGroupHandler struct {
	once    sync.Once
	cfg     *conf.KafkaConfig
	msgCh   chan *sarama.ConsumerMessage
	readyCh chan struct{}
	// 用于调控进程集合对主题和分区的分治
	consumerGroup sarama.ConsumerGroup
	closer        func()
}

// Msg 返回消费通道.
func (h *CustomConsumerGroupHandler) Msg() <-chan *sarama.ConsumerMessage {
	return h.msgCh
}

// Setup sarama.ConsumerGroupHandler接口定义实现.
// Setup() hook is called to notify the user of the claims and
// allow any necessary preparation or alteration of state.
func (h *CustomConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	close(h.readyCh)
	return nil
}

// Cleanup sarama.ConsumerGroupHandler接口定义实现.
// Cleanup() hook is called to allow the user to perform any final tasks
// before a rebalance once all the ConsumeClaim() loops have exited.
func (h *CustomConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim sarama.ConsumerGroupHandler接口定义实现.
// ConsumeClaim() hook is called for each of the assigned claims.
func (h *CustomConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			{
				log.Info().Msg("consumer group claim, context done")
				return nil
			}
		case msg, ok := <-claim.Messages():
			{
				if !ok {
					log.Warn().Msg("consumer group claim, messages channel closed")
					return nil
				}
				h.msgCh <- msg
				session.MarkMessage(msg, "")
			}
		}
	}
}

// Close 关闭并释放所有资源 (并发安全).
func (h *CustomConsumerGroupHandler) Close() {
	h.once.Do(func() { h.closer() })
	log.Info().Msg("unload kafka plugin")
}

// NewCustomConsumerGroupHandler 新建消费者.
func NewCustomConsumerGroupHandler(cfg *conf.KafkaConfig) (*CustomConsumerGroupHandler, error) {
	h := &CustomConsumerGroupHandler{
		cfg:     cfg,
		msgCh:   make(chan *sarama.ConsumerMessage),
		readyCh: make(chan struct{}),
	}

	var err error
	sc := sarama.NewConfig()
	sc.Version, err = sarama.ParseKafkaVersion(h.cfg.Version)
	if cfg.FromOldest {
		sc.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	if err != nil {
		log.Error().Err(err).Msgf("unsupported kafka version %s", h.cfg.Version)
		return nil, err
	}
	setKafkaAccessSettings(sc)

	h.consumerGroup, err = sarama.NewConsumerGroup(h.cfg.Brokers, h.cfg.ConsumeGroup, sc)
	if err != nil {
		log.Error().Err(err).Msgf("cannot create consumer group for brokers <%v>", h.cfg.Brokers)
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	kGroup := new(sync.WaitGroup)
	kGroup.Add(1)
	go func() {
		defer kGroup.Done()
		h.start(ctx, h.cfg.Topic)
	}()

	// 消费组准备完毕
	<-h.readyCh

	h.closer = func() {
		cancel()
		h.consumerGroup.Close()
		close(h.msgCh)
		kGroup.Wait()
	}

	log.Info().Msgf("load kafka plugin, version %s", h.cfg.Version)

	return h, nil
}

func (h *CustomConsumerGroupHandler) start(ctx context.Context, topics []string) {
	for {
		// Consume joins a cluster of consumers for a given list of topics and
		// starts a blocking ConsumerGroupSession through the ConsumerGroupHandler.
		if err := h.consumerGroup.Consume(ctx, topics, h); err != nil {
			log.Error().Err(err).Msg("error occurs inside consumer group")
		}

		// 1. The consumers join the group (as explained in https://kafka.apache.org/documentation/#intro_consumers)
		//    and is assigned their "fair share" of partitions, aka 'claims'.
		// 2. Before processing starts, the handler's Setup() hook is called to notify the user
		//    of the claims and allow any necessary preparation or alteration of state.
		// 3. For each of the assigned claims the handler's ConsumeClaim() function is then called
		//    in a separate goroutine which requires it to be thread-safe. Any state must be carefully protected
		//    from concurrent reads/writes.
		// 4. The session will persist until one of the ConsumeClaim() functions exits. This can be either when the
		//    parent context is cancelled or when a server-side rebalance cycle is initiated.
		// 5. Once all the ConsumeClaim() loops have exited, the handler's Cleanup() hook is called
		//    to allow the user to perform any final tasks before a rebalance.
		// 6. Finally, marked offsets are committed one last time before claims are released.
		//
		// Please note, that once a rebalance is triggered, sessions must be completed within
		// Config.Consumer.Group.Rebalance.Timeout. This means that ConsumeClaim() functions must exit
		// as quickly as possible to allow time for Cleanup() and the final offset commit. If the timeout
		// is exceeded, the consumer will be removed from the group by Kafka, which will cause offset
		// commit failures.
		select {
		case <-ctx.Done():
			{
				log.Info().Msg("consumer group context done ...")
				return
			}
		case err := <-h.consumerGroup.Errors():
			{
				log.Error().Err(err).Msg("error occurs inside consumer group")
			}
		default:
			{
				log.Info().Msg("rebalance after 2s ...")
				time.Sleep(time.Second * 2) // 防止刷屏
			}
		}
	}
}

func setKafkaAccessSettings(cfg *sarama.Config) {
	usr := os.Getenv("KAFKA_USERNAME")
	pwd := os.Getenv("KAFKA_PASSWORD")
	if usr == "" || pwd == "" {
		log.Info().Msg("access kafka without SASL settings")
		return
	}
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	cfg.Net.SASL.User = usr
	cfg.Net.SASL.Password = pwd
	cfg.Net.SASL.Version = sarama.SASLHandshakeV1
	log.Info().Msg("access kafka with SASL settings")
}
