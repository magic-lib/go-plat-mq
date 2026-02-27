package mq

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/magic-lib/go-plat-utils/conv"
	"net/http"
	"time"
)

// Event 代表一个通用的消息事件
type Event struct {
	Id        string      `json:"id"`        // 消息ID
	Topic     string      `json:"topic"`     // 消息主题
	Timestamp int64       `json:"timestamp"` // 消息创建时间戳，秒级
	Headers   http.Header `json:"headers"`   // 用于传递元数据，如 Trace Context
	Payload   any         `json:"payload"`   // 消息内容
}

// Publisher 定义了消息发布者的接口
type Publisher interface {
	// Publish 发布一个事件
	Publish(ctx context.Context, event *Event) (id string, err error)
	// Close 关闭发布者连接
	Close()
}

// ConsumerHandler 是处理消息的函数类型
type ConsumerHandler func(ctx context.Context, event *Event) error

// Consumer 定义了消息消费者的接口
type Consumer interface {
	// Subscribe 开始消费指定队列的消息
	Subscribe(topic string, handler ConsumerHandler) error
	// Close 关闭消费者连接
	Close()
}

func BuildEvent(event *Event) (*Event, error) {
	if event == nil {
		return nil, fmt.Errorf("event is empty")
	}
	if event.Topic == "" {
		return event, fmt.Errorf("topic is empty")
	}

	if event.Id == "" {
		event.Id = uuid.NewString()
	}

	if event.Timestamp == 0 {
		event.Timestamp = time.Now().Unix()
	}
	if event.Headers == nil {
		event.Headers = make(http.Header)
	}
	event.Headers.Set("Timestamp", conv.String(event.Timestamp))
	return event, nil
}

// PublishByType 实现
func PublishByType(ctx context.Context, b Publisher, topic string, t any) (string, error) {
	event := new(Event)
	var err error
	event.Topic = topic
	event.Payload = t
	event, err = BuildEvent(event)
	if err != nil {
		return "", err
	}
	return b.Publish(ctx, event)
}

// SubscribeByType 实现
func SubscribeByType[T any](b Consumer, topic string, handler func(t T) error) error {
	return b.Subscribe(topic, func(ctx context.Context, event *Event) error {
		data, err := conv.Convert[T](event.Payload)
		if err != nil {
			return fmt.Errorf("handler error for topic %s: %s, not type, error: %v", topic, event.Topic, err)
		}
		return handler(data)
	})
}

var (
	_ Publisher = (*LocalMessageQueue)(nil)
	_ Consumer  = (*LocalMessageQueue)(nil)
	_ Publisher = (*RedisMessageQueue)(nil)
	_ Consumer  = (*RedisMessageQueue)(nil)
)
