package mq

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/magic-lib/go-plat-utils/conn"
)

type internalTestPayload struct {
	Name string `json:"name"`
}

// newInternalBus 构造一个可用的 bus，Redis 不可用时跳过测试
func newInternalBus(t *testing.T, ns string, timeout time.Duration) *AsynqMessageQueue {
	t.Helper()
	cfg := &conn.Connect{Host: "127.0.0.1", Port: "6379"}
	bus, err := NewAsynqMessageQueue(cfg, &AsynqMessageQueue{
		Namespace: ns,
		Timeout:   timeout,
	})
	if err != nil {
		t.Skipf("redis not available, skip: %v", err)
	}
	return bus
}

// TestWaitResult_PollingOnly 验证「仅轮询、无 Pub/Sub」时仍能拿到结果。
//
// 背景：队列粒度已从 Namespace 改为 topicKey（={namespace}:{topic}），
// 但 inspector.GetTaskInfo(queue, id) 是单队列查找（先 checkQueueExists，再用 base.TaskKey(qname,id)）。
// 若仍用 b.Namespace 查询，队列不存在 → 恒返回 error → 轮询兜底静默失效，
// 一旦 Pub/Sub 消息丢失，调用方只能空等到超时。
//
// 这里显式传入 nil 的 resultCh，强制走纯轮询路径来暴露该问题。
func TestWaitResult_PollingOnly(t *testing.T) {
	bus := newInternalBus(t, "demo-polling", 15*time.Second)
	defer bus.Close()

	topic := "polling.only"
	if err := bus.Subscribe(topic, func(ctx context.Context, event *Event) (any, error) {
		return "pong", nil
	}); err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	event := &Event{
		Id:      uuid.NewString(),
		Topic:   topic,
		Payload: &internalTestPayload{Name: "ping"},
	}
	if _, err := bus.Publish(context.Background(), event); err != nil {
		t.Fatalf("publish error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// resultCh=nil → 强制纯轮询，验证兜底链路是否真的可用
	resp, err := bus.waitResult(ctx, event.Id, bus.getTopicKey(topic), nil)
	if err != nil {
		t.Fatalf("polling-only fallback failed (result would be lost on Pub/Sub miss): %v", err)
	}
	if resp == nil || resp.Data != "pong" {
		t.Fatalf("unexpected resp: %+v", resp)
	}
	t.Logf("polling-only fallback OK, resp: %+v", resp.Data)
}

// TestMultipleWorkersSameTopic 验证「多个 worker 进程注册相同 topic」的水平扩展场景。
// 期望：任务被分摊到多个 worker 并发处理，且每个任务**只被消费一次**（asynq 通过原子出队保证）。
func TestMultipleWorkersSameTopic(t *testing.T) {
	const (
		ns        = "demo-multi-worker"
		topic     = "shared.topic"
		workerNum = 3
		taskNum   = 15
	)

	var handled int64
	handler := func(ctx context.Context, event *Event) (any, error) {
		atomic.AddInt64(&handled, 1)
		return "ok", nil
	}

	// 启动多个 worker（模拟多进程），注册同一个 topic
	buses := make([]*AsynqMessageQueue, 0, workerNum)
	for i := 0; i < workerNum; i++ {
		bus := newInternalBus(t, ns, 20*time.Second)
		defer bus.Close()
		if err := bus.Subscribe(topic, handler); err != nil {
			t.Fatalf("worker %d subscribe error: %v", i, err)
		}
		buses = append(buses, bus)
	}
	// 等待各 worker 的 server 启动完成
	time.Sleep(1 * time.Second)

	// 由其中一个进程作为调用方，同步投递并等待结果
	for i := 0; i < taskNum; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		resp, err := buses[0].Request(ctx, &Event{
			Topic:   topic,
			Payload: &internalTestPayload{Name: fmt.Sprintf("task-%d", i)},
		})
		cancel()
		if err != nil {
			t.Fatalf("task %d failed: %v", i, err)
		}
		if resp == nil || resp.Data != "ok" {
			t.Fatalf("task %d unexpected resp: %+v", i, resp)
		}
	}

	// 关键：每个任务只能被处理一次，不能被多个 worker 重复消费
	if got := atomic.LoadInt64(&handled); got != taskNum {
		t.Fatalf("expected exactly %d handler executions (each task consumed once), got %d", taskNum, got)
	}
	t.Logf("%d workers shared topic %q: %d tasks all succeeded, each handled exactly once",
		workerNum, topic, taskNum)
}

// TestTopicQueueIsolation 验证队列隔离：不同 topic 的任务进入各自的队列，
// 不会被注册了其它 topic 的 worker 抢到（即不再出现 handler not found）。
func TestTopicQueueIsolation(t *testing.T) {
	const ns = "demo-isolation"

	// worker A 只注册 topicX
	workerA := newInternalBus(t, ns, 20*time.Second)
	defer workerA.Close()
	if err := workerA.Subscribe("topic.x", func(ctx context.Context, event *Event) (any, error) {
		return "from-A", nil
	}); err != nil {
		t.Fatalf("worker A subscribe error: %v", err)
	}

	// worker B 只注册 topicY（与 A 同 namespace，但 topic 不同）
	workerB := newInternalBus(t, ns, 20*time.Second)
	defer workerB.Close()
	if err := workerB.Subscribe("topic.y", func(ctx context.Context, event *Event) (any, error) {
		return "from-B", nil
	}); err != nil {
		t.Fatalf("worker B subscribe error: %v", err)
	}
	time.Sleep(1 * time.Second)

	// 投递 topicX 的任务：只应由 worker A 处理，绝不能被 worker B 抢到
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := workerA.Request(ctx, &Event{
		Topic:   "topic.x",
		Payload: &internalTestPayload{Name: "x"},
	})
	if err != nil {
		t.Fatalf("topic.x request failed (may have been stolen by a worker without this topic): %v", err)
	}
	if resp == nil || resp.Data != "from-A" {
		t.Fatalf("unexpected resp: %+v", resp)
	}

	// 反向验证：投递 topicY 的任务只应由 worker B 处理，绝不能被 worker A 抢到
	// （若队列未按 topic 隔离，这里会以 handler not found 重试直到超时失败）
	respY, err := workerB.Request(ctx, &Event{
		Topic:   "topic.y",
		Payload: &internalTestPayload{Name: "y"},
	})
	if err != nil {
		t.Fatalf("topic.y request failed (may have been stolen by a worker without this topic): %v", err)
	}
	if respY == nil || respY.Data != "from-B" {
		t.Fatalf("unexpected resp for topic.y: %+v", respY)
	}
	t.Logf("topic queue isolation OK, topic.x=%+v, topic.y=%+v", resp.Data, respY.Data)
}

// TestSubscribeAfterStart 验证「先 Start、后动态 Subscribe」仍能消费新 topic。
// asynq.Server 在 NewServer 时拷贝了队列配置，运行中新增队列不会自动生效，
// 因此 Subscribe 必须用 timer 防抖重启 server，否则新 topic 的任务会一直 pending。
func TestSubscribeAfterStart(t *testing.T) {
	const ns = "demo-dyn-sub"

	bus := newInternalBus(t, ns, 20*time.Second)
	defer bus.Close()

	if err := bus.Subscribe("dyn.a", func(ctx context.Context, event *Event) (any, error) {
		return "A", nil
	}); err != nil {
		t.Fatalf("subscribe dyn.a error: %v", err)
	}
	if err := bus.Start(); err != nil { // 显式启动，serverStarted=true
		t.Fatalf("start error: %v", err)
	}

	// server 启动之后再注册新 topic，等待防抖重启生效
	if err := bus.Subscribe("dyn.b", func(ctx context.Context, event *Event) (any, error) {
		return "B", nil
	}); err != nil {
		t.Fatalf("subscribe dyn.b error: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := bus.Request(ctx, &Event{
		Topic:   "dyn.b",
		Payload: &internalTestPayload{Name: "b"},
	})
	if err != nil {
		t.Fatalf("dyn.b request failed (new topic not consumed after Start): %v", err)
	}
	if resp == nil || resp.Data != "B" {
		t.Fatalf("unexpected resp: %+v", resp)
	}
	t.Logf("dynamic subscribe after Start OK, resp: %+v", resp.Data)
}
