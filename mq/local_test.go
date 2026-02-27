package mq_test

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-mq/mq"
	"testing"
	"time"
)

type TestEvent struct {
	Name string
}
type TestEvent2 struct {
	Age string
}

func TestLocalMessageQueue(t *testing.T) {
	busQue := mq.NewLocalMessageQueue(0)

	//监听不同的topic
	err := mq.SubscribeByType[*TestEvent](busQue, "test", func(event *TestEvent) error {
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

		id, err := mq.PublishByType(context.Background(), busQue, topic, payload)

		if err != nil {
			fmt.Println("publish error:", err)
		}
		fmt.Println("publish id:", id)
	}

	time.Sleep(10 * time.Second)

}
