package mq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/magic-lib/go-plat-mq/mq"
)

// ZamloanH5SessionEvent Kafka消息结构体
type ZamloanH5SessionEvent struct {
	Meta MetaInfo             `json:"meta"`
	Data ZamloanH5SessionData `json:"data"`
	Ext  map[string]any       `json:"ext"`
}

type MetaInfo struct {
	SchemaVersion      string `json:"schema_version"`
	SourceSystem       string `json:"source_system"`
	SourceTable        string `json:"source_table"`
	Env                string `json:"env"`
	EventTimeMs        int64  `json:"event_time_ms"`
	SourceCreateTimeMs int64  `json:"source_create_time_ms"`
	ProducerSendTimeMs int64  `json:"producer_send_time_ms"`
}

type ZamloanH5SessionData struct {
	SessionID        string `json:"session_id"`
	AnonymousID      string `json:"anonymous_id"`
	CreateTimeMs     int64  `json:"create_time_ms"`
	StartTimeMs      int64  `json:"start_time_ms"`
	EndTimeMs        *int64 `json:"end_time_ms"` // 允许null，使用指针
	EventName        string `json:"event_name"`
	PageName         string `json:"page_name"`
	Action           string `json:"action"`
	PageURL          string `json:"page_url"`
	SourceChannel    string `json:"source_channel"`
	OS               string `json:"os"`
	Browser          string `json:"browser"`
	DeviceType       string `json:"device_type"`
	NetworkType      string `json:"network_type"`
	Country          string `json:"country"`
	Mobile           string `json:"mobile"`
	MobileType       string `json:"mobile_type"`
	PreviousPage     string `json:"previous_page"`
	UserID           string `json:"user_id"`
	MemberID         string `json:"member_id"`
	UserLabel        string `json:"user_label"`
	CustomerType     string `json:"customer_type"`
	IsCreditEligible string `json:"is_credit_eligible"`
}

// TestNewKafkaMessageQueue_EmptyBrokers 空 broker 应返回错误
func TestNewKafkaMessageQueue_EmptyBrokers(t *testing.T) {
	_, err := mq.NewKafkaMessageQueue(&mq.KafkaMessageQueueConfig{
		Brokers: []string{},
		GroupID: "test-group",
	})
	if err == nil {
		t.Fatal("expected error for empty brokers, got nil")
	}
	t.Logf("empty brokers error: %v", err)

	_, err = mq.NewKafkaMessageQueue(nil)
	if err == nil {
		t.Fatal("expected error for nil config, got nil")
	}
	t.Logf("nil config error: %v", err)
}

// TestNewKafkaMessageQueue_InvalidBroker 无效 broker 地址应返回错误
func TestNewKafkaMessageQueue_InvalidBroker(t *testing.T) {
	_, err := mq.NewKafkaMessageQueue(&mq.KafkaMessageQueueConfig{
		Brokers: []string{"invalid-host:99999"},
		GroupID: "test-group",
		Timeout: 2 * time.Second,
	})
	if err == nil {
		t.Fatal("expected error for invalid broker, got nil")
	}
	t.Logf("invalid broker error: %v", err)
}

// TestKafkaMessageQueue_PublishAfterClose 关闭后发布应返回错误
func TestKafkaMessageQueue_PublishAfterClose(t *testing.T) {
	kq, err := mq.NewKafkaMessageQueue(&mq.KafkaMessageQueueConfig{
		Brokers: []string{"127.0.0.1:9092"},
		GroupID: "test-close",
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Skipf("kafka not available, skip: %v", err)
	}

	kq.Close()

	_, err = kq.Publish(context.Background(), &mq.Event{
		Topic:   "test.topic",
		Payload: "hello",
	})
	if err == nil {
		t.Fatal("expected error after close, got nil")
	}
	t.Logf("publish after close error: %v", err)
}

// TestKafkaMessageQueue_SubscribeAfterClose 关闭后订阅应返回错误
func TestKafkaMessageQueue_SubscribeAfterClose(t *testing.T) {
	kq, err := mq.NewKafkaMessageQueue(&mq.KafkaMessageQueueConfig{
		Brokers: []string{"127.0.0.1:9092"},
		GroupID: "test-close",
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Skipf("kafka not available, skip: %v", err)
	}

	kq.Close()

	err = kq.Subscribe("test.topic", func(ctx context.Context, event *mq.Event) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error after close, got nil")
	}
	t.Logf("subscribe after close error: %v", err)
}

// TestKafkaMessageQueue_DuplicateSubscribe 重复订阅同一 topic 应返回错误
func TestKafkaMessageQueue_DuplicateSubscribe(t *testing.T) {
	kq, err := mq.NewKafkaMessageQueue(&mq.KafkaMessageQueueConfig{
		Brokers: []string{"127.0.0.1:9092"},
		GroupID: "test-dup",
		Timeout: 2 * time.Second,
	})
	if err != nil {
		t.Skipf("kafka not available, skip: %v", err)
	}
	defer kq.Close()

	handler := func(ctx context.Context, event *mq.Event) error {
		return nil
	}

	err = kq.Subscribe("test.dup.topic", handler)
	if err != nil {
		t.Fatalf("first subscribe failed: %v", err)
	}

	err = kq.Subscribe("test.dup.topic", handler)
	if err == nil {
		t.Fatal("expected error for duplicate subscribe, got nil")
	}
	t.Logf("duplicate subscribe error: %v", err)
}

// TestKafkaMessageQueue_PublishAndSubscribe 端到端测试：发布消息后订阅消费
// 需要 Kafka 运行在 127.0.0.1:9092
func TestKafkaMessageQueue_PublishAndSubscribe(t *testing.T) {
	busQue, err := mq.NewKafkaMessageQueue(&mq.KafkaMessageQueueConfig{
		Brokers: []string{"121.91.156.50:29092"},
		GroupID: "my-service",
		Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Skipf("kafka not available, skip: %v", err)
	}

	// 订阅不同 topic
	err = mq.SubscribeByType[*TestEvent](busQue, "test", func(event *TestEvent) error {
		fmt.Println("receive event test:", event)
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe test error: %v", err)
	}

	err = mq.SubscribeByType[*TestEvent2](busQue, "test222", func(event *TestEvent2) error {
		fmt.Println("receive event test222:", event)
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe test222 error: %v", err)
	}

	// 发送消息
	for i := 0; i < 10; i++ {
		topic := ""
		var payload any
		if i%2 == 0 {
			topic = "test"
			payload = &TestEvent{
				Name: "name_" + fmt.Sprintf("%d", i),
			}
		} else {
			topic = "test222"
			payload = &TestEvent2{
				Age: "age_" + fmt.Sprintf("%d", i),
			}
		}

		id, err := mq.PublishByType(context.Background(), busQue, topic, payload)
		if err != nil {
			t.Errorf("publish error: %v", err)
		}
		t.Logf("publish id: %s", id)
	}

	time.Sleep(10 * time.Second)
	busQue.Close()
}

// TestKafkaMessageQueue_ConcurrentPublish 并发发布测试
func TestKafkaMessageQueue_ConcurrentPublish(t *testing.T) {
	kq, err := mq.NewKafkaMessageQueue(&mq.KafkaMessageQueueConfig{
		Brokers: []string{"121.91.156.50:29092"},
		Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Skipf("kafka not available, skip: %v", err)
	}
	defer kq.Close()

	id, err := kq.Publish(context.Background(), &mq.Event{
		Id:      fmt.Sprintf("msg-%d", 1),
		Topic:   "test",
		Payload: fmt.Sprintf("msg-key"),
	})
	if err != nil {
		t.Errorf("concurrent publish %d error: %v", 1, err)
	}
	t.Logf("concurrent publish %d id: %s", 1, id)
}

func TestKafkaMessageQueue_PublishAny(t *testing.T) {
	kq, err := mq.NewKafkaMessageQueue(&mq.KafkaMessageQueueConfig{
		Brokers: []string{"127.0.0.1:19092"},
		Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Skipf("kafka not available, skip: %v", err)
	}
	defer kq.Close()

	id, err := kq.PublishAny(context.Background(), &mq.Event{
		Id:    "msg-%d",
		Topic: "test",
		Payload: &ZamloanH5SessionEvent{
			Data: ZamloanH5SessionData{
				Action:  "action",
				PageURL: "http://127.0.0.1",
			},
		},
	})
	if err != nil {
		t.Errorf("concurrent publish %d error: %v", 1, err)
	}
	t.Logf("concurrent publish %d id: %s", 1, id)
}
