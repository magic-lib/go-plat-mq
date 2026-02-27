package mq

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-mq/mq/internal/bus"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"sync"
	"time"
)

// LocalMessageQueue 本地消息，单机使用
type LocalMessageQueue struct {
	// 使用 map 来存储不同主题的 Topic
	topics  map[string]*bus.Topic[*Event]
	timeout time.Duration
	mu      sync.RWMutex
	topicMu sync.Mutex
	closed  bool
}

// NewLocalMessageQueue 创建新的 LocalMessageQueue 实例
func NewLocalMessageQueue(timeout time.Duration) *LocalMessageQueue {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &LocalMessageQueue{
		timeout: timeout,
		topics:  make(map[string]*bus.Topic[*Event]),
	}
}

// getTopic 获取或创建指定主题的 Topic
func (b *LocalMessageQueue) getTopic(topic string) *bus.Topic[*Event] {
	b.topicMu.Lock()
	defer b.topicMu.Unlock()

	if t, exists := b.topics[topic]; exists {
		return t
	}

	t := bus.NewTopic[*Event]()
	b.topics[topic] = t
	return t
}

// Publish 实现 Publisher 接口
func (b *LocalMessageQueue) Publish(_ context.Context, event *Event) (id string, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return "", fmt.Errorf("bus is closed")
	}
	ev, err := BuildEvent(event)
	if err != nil {
		return "", err
	}
	newBus := b.getTopic(ev.Topic)
	// 发布事件
	newBus.PubAsync(ev)
	return ev.Id, nil
}

// Close 实现 Publisher 接口
func (b *LocalMessageQueue) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	b.closed = true
	b.topics = nil
}

// Subscribe 实现 Consumer 接口
func (b *LocalMessageQueue) Subscribe(topic string, handler ConsumerHandler) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return fmt.Errorf("bus is closed")
	}

	newBus := b.getTopic(topic)
	_ = newBus.Sub(func(event *Event) {
		goroutines.GoAsync(func(params ...any) {
			ctx, cancel := context.WithTimeout(context.Background(), b.timeout)
			defer cancel()

			if err := handler(ctx, event); err != nil {
				fmt.Printf("handler error for topic %s: %s, %v\n", topic, event.Topic, err)
			}
		})
	})
	return nil
}
