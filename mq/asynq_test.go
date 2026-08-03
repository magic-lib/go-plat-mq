package mq_test

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-mq/mq"
	"github.com/magic-lib/go-plat-utils/conn"
	"github.com/magic-lib/go-plat-utils/conv"
	"log"
	"sync"
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
	err = mq.SubscribeByType[*TestEvent](busQue, "test", func(event *TestEvent) (any, error) {
		fmt.Println("receive event test:", event)
		return nil, nil
	})
	if err != nil {
		fmt.Println("subscribe error:", err)
	}

	err = mq.SubscribeByType[*TestEvent2](busQue, "test222", func(event *TestEvent2) (any, error) {
		fmt.Println("receive event test222:", event)
		return nil, nil
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

// TestAsynqCall_Success 测试 Call 同步调用成功返回结果
// 需要 Redis 运行在 127.0.0.1:6379
func TestAsynqCall_Success(t *testing.T) {
	busQue, err := mq.NewAsynqMessageQueue(&conn.Connect{
		Host: "127.0.0.1",
		Port: "6379",
	}, &mq.AsynqMessageQueue{
		Namespace: "demo-call",
	})
	if err != nil {
		t.Skipf("redis not available, skip: %v", err)
	}
	defer busQue.Close()

	err = mq.SubscribeByType[*TestEvent](busQue, "call.test", func(event *TestEvent) (any, error) {
		t.Logf("handler received: Name=%s", event.Name)
		return nil, nil
	})
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := busQue.Request(ctx, &mq.Event{
		Topic:   "call.test",
		Payload: &TestEvent{Name: "hello-call"},
	})
	if err != nil {
		t.Fatalf("Call error: %v", err)
	}
	t.Logf("Call result: %+v", result)

	if result == nil {
		t.Fatal("expected non-nil result, got nil")
	}
}

// TestAsynqCall_HandlerError 测试 handler 返回 error 时，Call 应拿到错误结果
func TestAsynqCall_HandlerError(t *testing.T) {
	busQue, err := mq.NewAsynqMessageQueue(&conn.Connect{
		Host: "127.0.0.1",
		Port: "6379",
	}, &mq.AsynqMessageQueue{
		Namespace: "demo-call",
	})
	if err != nil {
		t.Skipf("redis not available, skip: %v", err)
	}
	defer busQue.Close()

	expectedErr := fmt.Errorf("business error: order not found")
	err = mq.SubscribeByType[*TestEvent](busQue, "call.error", func(event *TestEvent) (any, error) {
		t.Logf("handler received: Name=%s, returning error", event.Name)
		return nil, expectedErr
	})
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := busQue.Request(ctx, &mq.Event{
		Topic:   "call.error",
		Payload: &TestEvent{Name: "error-case"},
	})
	if err == nil {
		t.Fatal("expected error from Call, got nil")
	} else {
		t.Fatal("expected error from Call, got err", err.Error())
	}
	t.Logf("Call error result: %+v, err: %v", result, err)
}

// TestAsynqCall_Closed 测试 queue 关闭后 Call 应报错
func TestAsynqCall_Closed(t *testing.T) {
	busQue, err := mq.NewAsynqMessageQueue(&conn.Connect{
		Host: "127.0.0.1",
		Port: "6379",
	}, &mq.AsynqMessageQueue{
		Namespace: "demo-call",
	})
	if err != nil {
		t.Skipf("redis not available, skip: %v", err)
	}
	busQue.Close()

	_, err = busQue.Request(context.Background(), &mq.Event{
		Topic:   "call.test",
		Payload: &TestEvent{Name: "after-close"},
	})
	if err == nil {
		t.Fatal("expected error after close, got nil")
	}
	t.Logf("Call after close error: %v", err)
}

// TestAsynqCall_Timeout 测试 Call 超时：handler 耗时超过 context deadline
func TestAsynqCall_Timeout(t *testing.T) {
	busQue, err := mq.NewAsynqMessageQueue(&conn.Connect{
		Host: "127.0.0.1",
		Port: "6379",
	}, &mq.AsynqMessageQueue{
		Namespace: "demo-call",
		Timeout:   10 * time.Second,
	})
	if err != nil {
		t.Skipf("redis not available, skip: %v", err)
	}
	defer busQue.Close()

	err = mq.SubscribeByType[*TestEvent](busQue, "call.slow", func(event *TestEvent) (any, error) {
		t.Logf("handler sleeping 3s: Name=%s", event.Name)
		time.Sleep(3 * time.Second)
		return nil, nil
	})
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err = busQue.Request(ctx, &mq.Event{
		Topic:   "call.slow",
		Payload: &TestEvent{Name: "slow-task"},
	})
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	t.Logf("Call timeout error: %v", err)
}

// TestAsynqCall_Concurrent 测试并发 Call：多个 goroutine 同时同步调用
func TestAsynqCall_Concurrent(t *testing.T) {
	busQue, err := mq.NewAsynqMessageQueue(&conn.Connect{
		Host: "127.0.0.1",
		Port: "6379",
	}, &mq.AsynqMessageQueue{
		Namespace: "demo-call",
		Timeout:   10 * time.Second,
	})
	if err != nil {
		t.Skipf("redis not available, skip: %v", err)
	}
	defer busQue.Close()

	err = mq.SubscribeByType[*TestEvent](busQue, "call.concurrent", func(event *TestEvent) (any, error) {
		t.Logf("handler processing: Name=%s", event.Name)
		time.Sleep(500 * time.Millisecond)
		if event.Name == "task-2" {
			return nil, fmt.Errorf("business error: order 2 not found")
		}
		return event.Name, nil
	})
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	concurrency := 5
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			result, callErr := busQue.Request(ctx, &mq.Event{
				Topic:   "call.concurrent",
				Payload: &TestEvent{Name: fmt.Sprintf("task-%d", idx)},
			})
			if callErr != nil {
				log.Println("callErr error:", callErr)
				return
			}
			t.Logf("goroutine %d: Call result: %s", idx, conv.String(result))
		}(i)
	}
	wg.Wait()
	close(errCh)

	for e := range errCh {
		t.Error(e)
	}
}
