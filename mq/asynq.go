package mq

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-utils/utils/httputil"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/redis/go-redis/v9"
	"log"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/magic-lib/go-plat-utils/conn"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/goroutines"
)

// AsynqMessageQueue 基于 asynq（Redis 后端）实现的消息队列
type AsynqMessageQueue struct {
	Namespace    string
	Timeout      time.Duration
	ServerConfig *asynq.Config // 消费端配置

	redisOpt         *asynq.RedisClientOpt
	redisClient      *redis.Client // 用于 Pub/Sub 实时推送结果（跨进程/分布式可用）
	pushClient       *asynq.Client
	subServer        *asynq.Server
	serverStarted    bool
	mainMux          *asynq.ServeMux
	subscribedTopics cmap.ConcurrentMap[string, bool]
	pushTypeTopics   cmap.ConcurrentMap[string, reflect.Type]
	topicMu          sync.Mutex
	mu               sync.RWMutex
	closed           bool
}

// resultChannel 返回某个任务结果推送的 Redis channel 名
func (b *AsynqMessageQueue) resultChannel(taskID string) string {
	return fmt.Sprintf("mq:result:%s:%s", b.Namespace, taskID)
}

// NewAsynqMessageQueue 创建新的 AsynqMessageQueue 实例
func NewAsynqMessageQueue(cfg *conn.Connect, mqConf *AsynqMessageQueue) (*AsynqMessageQueue, error) {
	if cfg == nil || cfg.Host == "" || cfg.Port == "" {
		return nil, fmt.Errorf("redis config error")
	}
	db := 0
	workerNum := 10
	defaultNamespace := "default"
	defaultTimeout := time.Second * 5
	if cfg.Database != "" {
		dbTemp, err := conv.Convert[int](cfg.Database)
		if err == nil {
			db = dbTemp
		}
	}
	redisOpt := asynq.RedisClientOpt{
		Addr:     net.JoinHostPort(cfg.Host, cfg.Port),
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       db,
	}
	client := asynq.NewClient(redisOpt)
	if mqConf == nil {
		mqConf = &AsynqMessageQueue{}
	}
	mqConf.pushClient = client
	mqConf.redisOpt = &redisOpt
	// Pub/Sub 需要独立的连接（subscribe 会占用连接，不能与命令复用同一连接池的常见模式冲突）
	mqConf.redisClient = redis.NewClient(&redis.Options{
		Addr:     net.JoinHostPort(cfg.Host, cfg.Port),
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       db,
	})
	if mqConf.Timeout <= 0 {
		mqConf.Timeout = defaultTimeout
	}
	if mqConf.Namespace == "" {
		mqConf.Namespace = defaultNamespace
	}
	if mqConf.ServerConfig == nil {
		mqConf.ServerConfig = &asynq.Config{
			Concurrency: workerNum,
			Queues: map[string]int{
				mqConf.Namespace: workerNum,
			},
		}
	}

	if mqConf.ServerConfig.Queues == nil {
		mqConf.ServerConfig.Queues = make(map[string]int)
	}
	if _, exists := mqConf.ServerConfig.Queues[mqConf.Namespace]; !exists {
		mqConf.ServerConfig.Queues[mqConf.Namespace] = workerNum
	}

	mqConf.pushTypeTopics = cmap.New[reflect.Type]()
	mqConf.subscribedTopics = cmap.New[bool]()
	mqConf.mainMux = asynq.NewServeMux()

	return mqConf, client.Ping()
}

func (b *AsynqMessageQueue) getTopicKey(topic string) string {
	return fmt.Sprintf("%s:%s", b.Namespace, topic)
}

func (b *AsynqMessageQueue) handleTopic(topic string, handleTask func(context.Context, *asynq.Task) error) bool {
	b.topicMu.Lock()
	defer b.topicMu.Unlock()

	topicKey := b.getTopicKey(topic)

	if exists := b.subscribedTopics.Has(topicKey); exists {
		return false
	}

	b.subscribedTopics.Set(topicKey, true)

	b.mainMux.HandleFunc(topicKey, handleTask)

	return true
}

// Publish 实现 Publisher 接口
func (b *AsynqMessageQueue) Publish(ctx context.Context, event *Event) (id string, err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return "", fmt.Errorf("bus is closed")
	}
	ev, err := BuildEvent(event)
	if err != nil {
		return "", err
	}
	evString := conv.String(ev)

	topicKey := b.getTopicKey(event.Topic)

	if oneType, ok := b.pushTypeTopics.Get(topicKey); ok {
		currType := reflect.TypeOf(ev.Payload)
		if currType.String() != oneType.String() {
			log.Printf("error: push type error %s: %s, not type: %s, %s, value: %v \n", topicKey, ev.Topic, oneType.String(), currType.String(), ev.Payload)
		}
	} else {
		b.pushTypeTopics.Set(topicKey, reflect.TypeOf(ev.Payload))
	}

	task := asynq.NewTask(topicKey, []byte(evString))
	info, err := b.pushClient.EnqueueContext(ctx, task,
		asynq.TaskID(event.Id),
		asynq.Queue(b.Namespace),
		asynq.Timeout(b.Timeout),
		asynq.Retention(b.Timeout),
	)
	if err != nil {
		return "", fmt.Errorf("enqueue task failed: %v", err)
	}
	return info.ID, nil
}

// Close 实现 Publisher 接口
func (b *AsynqMessageQueue) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}

	b.closed = true
	_ = b.pushClient.Close()
	if b.redisClient != nil {
		_ = b.redisClient.Close()
	}
	if b.subServer != nil {
		b.subServer.Shutdown()
	}
}

// Subscribe 实现 Consumer 接口
func (b *AsynqMessageQueue) Subscribe(topic string, handler ConsumerHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("bus is closed")
	}
	if b.subServer == nil {
		b.subServer = asynq.NewServer(b.redisOpt, *b.ServerConfig)
	}
	isNew := b.handleTopic(topic, func(ctx context.Context, task *asynq.Task) error {
		// 格式错误，直接返回nil，不用重试
		topicKey := b.getTopicKey(topic)
		if task.Type() != topicKey {
			log.Printf("handler error for topic %s: %s, not type", topicKey, task.Type())
			return nil
		}
		ev, err := conv.Convert[*Event](task.Payload())
		if err != nil {
			log.Printf("handler error for topic %s: %s, not type, error: %v", topic, task.Type(), err)
			return nil
		}
		// 执行用户 handler
		result, handlerErr := handler(ctx, ev)

		resp := new(httputil.CommResponse)
		resp.Params = ev
		resp.Data = result
		if handlerErr != nil {
			resp.Code = http.StatusInternalServerError
			resp.Message = handlerErr.Error()
		}
		// 将执行结果写入 ResultWriter（Call 同步等待需要）
		if rw := task.ResultWriter(); rw != nil {
			respString := conv.String(resp)
			_, _ = rw.Write([]byte(respString))

			// 通过 Redis Pub/Sub 实时推送结果，唤醒等待中的 Request（支持分布式）
			if b.redisClient != nil {
				if pubErr := b.redisClient.Publish(ctx, b.resultChannel(ev.Id), respString).Err(); pubErr != nil {
					log.Printf("publish result to redis pub/sub failed for task %s: %v", ev.Id, pubErr)
				}
			}
		}
		// 业务错误不重试：用 asynq.SkipRetry 哨兵错误包装，避免 asynq 默认重试（MaxRetry=25）
		// 业务错误通常是确定性的，重试无意义，且会让 Call 调用方长时间阻塞
		if handlerErr != nil {
			//return fmt.Errorf("%w: %v", asynq.SkipRetry, handlerErr)
			log.Printf("%w: %v", asynq.SkipRetry, handlerErr)
			return nil
		}
		return nil
	})
	if !isNew {
		return fmt.Errorf("topic %s sub already start", topic)
	}

	if !b.serverStarted {
		b.serverStarted = true
		goroutines.GoAsync(func(params ...any) {
			if err := b.subServer.Start(b.mainMux); err != nil {
				log.Println("start asynq server error:", err)
			}
		})
	}
	return nil
}

// Request 同步提交任务并等待 Consumer 处理完毕，实时返回执行结果
// 类似 HTTP 请求-响应模式，会阻塞直到任务完成或超时，返回any
// 实现方式：Redis Pub/Sub 推模型（支持分布式）+ Inspector 兜底查询（解决订阅前完成的时间窗口）
func (b *AsynqMessageQueue) Request(ctx context.Context, event *Event) (*httputil.CommResponse, error) {
	taskID, err := b.Publish(ctx, event)
	if err != nil {
		return nil, err
	}

	// 1. 订阅结果 channel（必须在 Publish 之后、任何等待之前立即订阅）
	pubsub := b.redisClient.Subscribe(ctx, b.resultChannel(taskID))
	defer func() {
		_ = pubsub.Close()
	}()
	resultCh := pubsub.Channel()

	// 2. 兜底：订阅后立刻用 Inspector 查一次，覆盖「Consumer 在订阅前已完成」的极小窗口
	inspector := asynq.NewInspector(b.redisOpt)
	defer func() {
		_ = inspector.Close()
	}()
	if resp, done, derr := b.checkTaskResult(inspector, taskID); done {
		return resp, derr
	}

	// 3. 等待：要么从 Pub/Sub 收到实时推送，要么超时/取消
	deadline := time.Now().Add(b.Timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, fmt.Errorf("call: wait result timeout for task %s", taskID)
		case msg, ok := <-resultCh:
			// Consumer 完成并通过 Pub/Sub 推送了结果，零延迟返回
			if !ok {
				// Pub/Sub 连接异常关闭，退化为 Inspector 轮询兜底
				return b.waitByInspector(inspector, ctx, taskID, deadline)
			}
			resp := &httputil.CommResponse{}
			if len(msg.Payload) > 0 {
				_ = conv.Unmarshal([]byte(msg.Payload), resp)
			}
			if resp.Message != "" {
				return resp, fmt.Errorf("%s", resp.Message)
			}
			return resp, nil
		}
	}
}

// checkTaskResult 用 Inspector 查一次任务结果，done=true 表示已终态可返回
func (b *AsynqMessageQueue) checkTaskResult(inspector *asynq.Inspector, taskID string) (*httputil.CommResponse, bool, error) {
	taskInfo, err := inspector.GetTaskInfo(b.Namespace, taskID)
	if err != nil {
		// 任务尚未落库（仍在队列中未开始处理），属于正常情况，继续等待 Pub/Sub
		return nil, false, nil
	}
	switch taskInfo.State {
	case asynq.TaskStateCompleted:
		resp := &httputil.CommResponse{}
		if len(taskInfo.Result) > 0 {
			_ = conv.Unmarshal(taskInfo.Result, resp)
		}
		if resp.Message != "" {
			return resp, true, fmt.Errorf("%s", resp.Message)
		}
		return resp, true, nil
	case asynq.TaskStateRetry:
		resp := &httputil.CommResponse{}
		if len(taskInfo.Result) > 0 {
			_ = conv.Unmarshal(taskInfo.Result, resp)
		}
		if resp.Message != "" {
			return resp, true, fmt.Errorf("%s", resp.Message)
		}
		return nil, true, fmt.Errorf("task retry: %s", taskInfo.LastErr)
	case asynq.TaskStateArchived:
		return nil, true, fmt.Errorf("task archived: %s", string(taskInfo.Result))
	default:
		// 其他状态（active/pending/scheduled）继续等待 Pub/Sub 推送
		return nil, false, nil
	}
}

// waitByInspector Pub/Sub 失效时的兜底轮询，保证可靠性
func (b *AsynqMessageQueue) waitByInspector(inspector *asynq.Inspector, ctx context.Context, taskID string, deadline time.Time) (*httputil.CommResponse, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if resp, done, derr := b.checkTaskResult(inspector, taskID); done {
			return resp, derr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("call: wait result timeout for task %s", taskID)
			}
		}
	}
}
