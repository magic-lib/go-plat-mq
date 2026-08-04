package mq

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-utils/utils/httputil"
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
	pushClient       *asynq.Client
	subServer        *asynq.Server
	serverStarted    bool
	mainMux          *asynq.ServeMux
	subscribedTopics map[string]bool
	pushTypeTopics   map[string]reflect.Type
	topicMu          sync.Mutex
	mu               sync.RWMutex
	closed           bool
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

	mqConf.pushTypeTopics = make(map[string]reflect.Type)
	mqConf.subscribedTopics = make(map[string]bool)
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

	if _, exists := b.subscribedTopics[topicKey]; exists {
		return false
	}

	b.subscribedTopics[topicKey] = true

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

	if oneType, ok := b.pushTypeTopics[topicKey]; ok {
		currType := reflect.TypeOf(ev.Payload)
		if currType.String() != oneType.String() {
			log.Printf("error: push type error %s: %s, not type: %s, %s, value: %v \n", topicKey, ev.Topic, oneType.String(), currType.String(), ev.Payload)
		}
	} else {
		b.pushTypeTopics[topicKey] = reflect.TypeOf(ev.Payload)
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
		}
		// 业务错误不重试：用 asynq.SkipRetry 哨兵错误包装，避免 asynq 默认重试（MaxRetry=25）
		// 业务错误通常是确定性的，重试无意义，且会让 Call 调用方长时间阻塞
		if handlerErr != nil {
			return fmt.Errorf("%w: %v", asynq.SkipRetry, handlerErr)
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
func (b *AsynqMessageQueue) Request(ctx context.Context, event *Event) (*httputil.CommResponse, error) {
	taskID, err := b.Publish(ctx, event)
	if err != nil {
		return nil, err
	}

	// 使用 Inspector 轮询等待任务完成
	inspector := asynq.NewInspector(b.redisOpt)
	defer func() {
		_ = inspector.Close()
	}()

	ticker := time.NewTicker(50 * time.Millisecond) // 轮询间隔
	defer ticker.Stop()

	deadline := time.Now().Add(b.Timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("call: wait result timeout for task %s", taskID)
			}

			taskInfo, err := inspector.GetTaskInfo(b.Namespace, taskID)
			if err != nil {
				// 任务可能还在队列中未开始处理，这是正常的，继续等待
				continue
			}

			switch taskInfo.State {
			case asynq.TaskStateCompleted:
				resp := &httputil.CommResponse{}
				if len(taskInfo.Result) > 0 {
					_ = conv.Unmarshal(taskInfo.Result, resp)
				}
				if resp.Message != "" {
					return resp, fmt.Errorf("%s", resp.Message)
				}
				return resp, nil
			case asynq.TaskStateRetry:
				resp := &httputil.CommResponse{}
				if len(taskInfo.Result) > 0 {
					_ = conv.Unmarshal(taskInfo.Result, resp)
				}
				if resp.Message != "" {
					return resp, fmt.Errorf("%s", resp.Message)
				}
				return nil, fmt.Errorf("task retry: %s", string(taskInfo.Result))
			case asynq.TaskStateArchived:
				return nil, fmt.Errorf("task archived: %s", string(taskInfo.Result))
			default:
				continue
			}
			// 其他状态（active/pending/scheduled/retry）继续等待
		}
	}
}
