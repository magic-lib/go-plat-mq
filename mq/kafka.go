package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/magic-lib/go-plat-utils/goroutines"
	cmap "github.com/orcaman/concurrent-map/v2"
)

// KafkaMessageQueueConfig Kafka 消息队列配置
type KafkaMessageQueueConfig struct {
	Brokers []string      // Kafka broker 地址列表
	GroupID string        // 消费组 ID，为空时自动生成
	Timeout time.Duration // 消费处理超时，默认 30s
}

// kafkaClientSet 共享的 Kafka 底层连接（按 brokers 缓存复用）
type kafkaClientSet struct {
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	saramaConfig  *sarama.Config
	ctx           context.Context
	ctxCancel     context.CancelFunc
	closeOnce     sync.Once
}

// KafkaMessageQueue Kafka 消息队列，实现 Publisher 和 Consumer 接口
type KafkaMessageQueue struct {
	cfg       *KafkaMessageQueueConfig
	clientSet *kafkaClientSet

	subscribedTopics map[string]bool
	topicMu          sync.Mutex
	mu               sync.RWMutex
	closed           bool
}

var (
	kafkaClientSets = cmap.New[*kafkaClientSet]()
)

// brokersKey 将 brokers 排序后生成唯一 key，用于 cmap 缓存
func brokersKey(brokers []string) string {
	sorted := make([]string, len(brokers))
	copy(sorted, brokers)
	sort.Strings(sorted)
	return strings.Join(sorted, ",")
}

// getOrCreateClientSet 获取或创建共享的 kafka 底层连接
func getOrCreateClientSet(cfg *KafkaMessageQueueConfig) (*kafkaClientSet, error) {
	key := brokersKey(cfg.Brokers)

	if cs, ok := kafkaClientSets.Get(key); ok {
		return cs, nil
	}

	saramaCfg := sarama.NewConfig()
	saramaCfg.Net.DialTimeout = 3 * time.Second
	saramaCfg.Net.ReadTimeout = 5 * time.Second
	saramaCfg.Net.WriteTimeout = 5 * time.Second
	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	saramaCfg.Producer.Retry.Max = 3
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	syncPro, err := sarama.NewSyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("kafka: create sync producer failed: %w", err)
	}

	asyncPro, err := sarama.NewAsyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		_ = syncPro.Close()
		return nil, fmt.Errorf("kafka: create async producer failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	cs := &kafkaClientSet{
		syncProducer:  syncPro,
		asyncProducer: asyncPro,
		saramaConfig:  saramaCfg,
		ctx:           ctx,
		ctxCancel:     cancel,
	}

	// 异步错误监听
	goroutines.GoAsync(func(params ...any) {
		for {
			select {
			case err, ok := <-asyncPro.Errors():
				if !ok {
					return
				}
				log.Printf("kafka async send error: %v", err)
			case <-ctx.Done():
				return
			}
		}
	})

	kafkaClientSets.Set(key, cs)
	return cs, nil
}

// NewKafkaMessageQueue 创建 Kafka 消息队列实例
func NewKafkaMessageQueue(cfg *KafkaMessageQueueConfig) (*KafkaMessageQueue, error) {
	if cfg == nil || len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: brokers is empty")
	}

	cs, err := getOrCreateClientSet(cfg)
	if err != nil {
		return nil, err
	}

	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}

	return &KafkaMessageQueue{
		cfg:              cfg,
		clientSet:        cs,
		subscribedTopics: make(map[string]bool),
	}, nil
}

// Publish 实现 Publisher 接口：将 Event 序列化为 JSON 后同步发送到 Kafka
func (k *KafkaMessageQueue) Publish(ctx context.Context, event *Event) (id string, err error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.closed {
		return "", fmt.Errorf("kafka: queue is closed")
	}

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	ev, err := BuildEvent(event)
	if err != nil {
		return "", err
	}

	payload, err := json.Marshal(ev)
	if err != nil {
		return "", fmt.Errorf("kafka: marshal event failed: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: ev.Topic,
		Key:   sarama.StringEncoder(ev.Id),
		Value: sarama.ByteEncoder(payload),
	}

	_, _, err = k.clientSet.syncProducer.SendMessage(msg)
	if err != nil {
		return ev.Id, fmt.Errorf("kafka: send message failed: %w", err)
	}

	return ev.Id, nil
}

// PublishAny 将任意结构体Payload序列化为 JSON 后发送到 Kafka 指定 topic
func (k *KafkaMessageQueue) PublishAny(ctx context.Context, event *Event) (string, error) {
	if event == nil {
		return "", fmt.Errorf("kafka: event is nil")
	}
	topic := event.Topic
	id := event.Id
	data := event.Payload
	return k.publishAny(ctx, topic, id, data)
}

func (k *KafkaMessageQueue) publishAny(ctx context.Context, topic string, id string, data any) (string, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.closed {
		return "", fmt.Errorf("kafka: queue is closed")
	}

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	if topic == "" {
		return "", fmt.Errorf("kafka: topic is empty")
	}

	if id == "" {
		id = uuid.NewString()
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("kafka: marshal data failed: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(id),
		Value: sarama.ByteEncoder(payload),
	}

	_, _, err = k.clientSet.syncProducer.SendMessage(msg)
	if err != nil {
		return id, fmt.Errorf("kafka: send message failed: %w", err)
	}

	return id, nil
}

// Subscribe 实现 Consumer 接口：启动后台消费 goroutine
func (k *KafkaMessageQueue) Subscribe(topic string, handler ConsumerHandler) error {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.closed {
		return fmt.Errorf("kafka: queue is closed")
	}

	k.topicMu.Lock()
	if k.subscribedTopics[topic] {
		k.topicMu.Unlock()
		return fmt.Errorf("kafka: topic %s already subscribed", topic)
	}
	k.subscribedTopics[topic] = true
	k.topicMu.Unlock()

	groupID := k.cfg.GroupID
	if groupID == "" {
		groupID = fmt.Sprintf("mq-kafka-%s", topic)
	}

	goroutines.GoAsync(func(params ...any) {
		cgHandler := &kafkaConsumerGroupHandler{handler: handler}
		for {
			select {
			case <-k.clientSet.ctx.Done():
				return
			default:
			}

			consumer, err := sarama.NewConsumerGroup(k.cfg.Brokers, groupID, k.clientSet.saramaConfig)
			if err != nil {
				log.Printf("kafka: create consumer group [%s] failed: %v", groupID, err)
				time.Sleep(time.Second)
				continue
			}

			err = consumer.Consume(k.clientSet.ctx, []string{topic}, cgHandler)
			if err != nil {
				log.Printf("kafka: consume topic [%s] error: %v", topic, err)
			}
			_ = consumer.Close()

			select {
			case <-k.clientSet.ctx.Done():
				return
			default:
			}
			time.Sleep(time.Second)
		}
	})

	return nil
}

// Close 实现 Publisher / Consumer 接口：标记关闭（共享连接不在此关闭）
func (k *KafkaMessageQueue) Close() {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.closed {
		return
	}
	k.closed = true
}

// kafkaConsumerGroupHandler 将 sarama 消费组消息适配为 mq.Event 回调
type kafkaConsumerGroupHandler struct {
	handler ConsumerHandler
}

func (h *kafkaConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *kafkaConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *kafkaConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var ev Event
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			log.Printf("kafka: unmarshal event failed topic=%s offset=%d: %v", msg.Topic, msg.Offset, err)
			sess.MarkMessage(msg, "")
			continue
		}
		if err := h.handler(sess.Context(), &ev); err != nil {
			log.Printf("kafka: handle event failed topic=%s offset=%d err=%v", msg.Topic, msg.Offset, err)
		} else {
			sess.MarkMessage(msg, "")
		}
	}
	return nil
}
