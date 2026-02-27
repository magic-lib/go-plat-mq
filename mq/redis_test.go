package mq_test

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-mq/mq"
	"github.com/magic-lib/go-plat-utils/conn"
	"testing"
	"time"
)

func TestRedisMessageQueue(t *testing.T) {
	busQue, err := mq.NewRedisMessageQueue(&conn.Connect{
		Host:     "192.168.2.84",
		Port:     "6379",
		Password: "mjhttyryt565-jyjh5824t-p55w",
	}, &mq.RedisMessageQueue{
		Namespace: "demo",
	})
	if err != nil {
		fmt.Println("new redis message queue error:", err)
		return
	}

	//监听不同的topic
	err = mq.SubscribeByType[*TestEvent](busQue, "test", func(event *TestEvent) error {
		fmt.Println("receive event test:", event)
		// 如果返回error，则消息会被重新发送
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

		id, err := mq.PublishByType(context.Background(), busQue, topic, payload)

		if err != nil {
			fmt.Println("publish error:", err)
		}
		fmt.Println("publish id:", id)
	}

	time.Sleep(10 * time.Second)
	busQue.Close()

}
