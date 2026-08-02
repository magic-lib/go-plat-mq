package mq_test

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-mq/mq"
	"github.com/magic-lib/go-plat-utils/conn"
	"testing"
	"time"
)

func TestAsynqMessageQueue(t *testing.T) {
	busQue, err := mq.NewAsynqMessageQueue(&conn.Connect{
		Host: "127.0.0.1",
		Port: "6379",
	}, &mq.AsynqMessageQueue{
		Namespace: "demo",
	})
	if err != nil {
		fmt.Println("new asynq message queue error:", err)
		return
	}

	// 监听不同的topic
	err = mq.SubscribeByType[*TestEvent](busQue, "test", func(event *TestEvent) error {
		fmt.Println("receive event test:", event)
		return nil
	})
	if err != nil {
		fmt.Println("subscribe error:", err)
	}

	err = mq.SubscribeByType[*TestEvent2](busQue, "test222", func(event *TestEvent2) error {
		fmt.Println("receive event test222:", event)
		return nil
	})
	if err != nil {
		fmt.Println("subscribe222 error:", err)
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
		if i == 7 {
			topic = "test222"
			payload = &TestEvent{
				Name: "name_" + fmt.Sprintf("%d", i),
			}
		}

		id, err := busQue.Publish(context.Background(), &mq.Event{
			Topic:   topic,
			Payload: payload,
		})
		if err != nil {
			fmt.Println("publish error:", err)
		}
		fmt.Println("publish id:", id)
	}

	time.Sleep(10 * time.Second)
	busQue.Close()
}
