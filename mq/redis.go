package mq

import (
	"context"
	"fmt"
	"github.com/hibiken/asynq"
	"github.com/magic-lib/go-omniflow/sasynq"
	"github.com/magic-lib/go-plat-utils/conn"
	"github.com/magic-lib/go-plat-utils/conv"
	"github.com/magic-lib/go-plat-utils/goroutines"
	"net"
	"reflect"
	"sync"
	"time"
)

// RedisMessageQueue redis消息
type RedisMessageQueue struct {
	Namespace    string
	Timeout      time.Duration
	ServerConfig *asynq.Config //消费端配置

	redisOpt         *asynq.RedisClientOpt
	pushClient       *asynq.Client
	subServer        *asynq.Server
	serverStarted    bool            // 标记server是否已启动
	mainMux          *asynq.ServeMux // 主路由处理
	subscribedTopics map[string]bool
	pushTypeTopics   map[string]reflect.Type
	topicMu          sync.Mutex
	mu               sync.RWMutex
	closed           bool
}

// NewRedisMessageQueue 创建新的 RedisMessageQueue 实例
func NewRedisMessageQueue(cfg *conn.Connect, mqConf *RedisMessageQueue) (*RedisMessageQueue, error) {
	if cfg.Host == "" || cfg.Port == "" {
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
		mqConf = &RedisMessageQueue{}
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

func (b *RedisMessageQueue) getTopicKey(topic string) string {
	return fmt.Sprintf("%s:%s", b.Namespace, topic)
}

func (b *RedisMessageQueue) getTopic(topic string, handleTask func(context.Context, *asynq.Task) error) bool {
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
func (b *RedisMessageQueue) Publish(ctx context.Context, event *Event) (id string, err error) {
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
			fmt.Printf("error: push type error %s: %s, not type: %s, %s, value: %v \n", topicKey, ev.Topic, oneType.String(), currType.String(), ev.Payload)
		}
	} else {
		b.pushTypeTopics[topicKey] = reflect.TypeOf(ev.Payload)
	}

	task := asynq.NewTask(topicKey, []byte(evString))
	info, err := b.pushClient.EnqueueContext(ctx, task,
		sasynq.WithUniqueID(event.Id),
		sasynq.WithQueue(b.Namespace),
		sasynq.WithTimeout(b.Timeout),
	)
	if err != nil {
		return "", fmt.Errorf("enqueue task failed: %v", err)
	}
	return info.ID, nil
}

// Close 实现 Publisher 接口
func (b *RedisMessageQueue) Close() {
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
func (b *RedisMessageQueue) Subscribe(topic string, handler ConsumerHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("bus is closed")
	}
	if b.subServer == nil {
		b.subServer = asynq.NewServer(b.redisOpt, *b.ServerConfig)
	}
	isNew := b.getTopic(topic, func(ctx context.Context, task *asynq.Task) error {
		// 格式错误，直接返回nil，不用重试
		topicKey := b.getTopicKey(topic)
		if task.Type() != topicKey {
			fmt.Printf("handler error for topic %s: %s, not type", topicKey, task.Type())
			return nil
		}
		ev, err := conv.Convert[*Event](task.Payload())
		if err != nil {
			fmt.Printf("handler error for topic %s: %s, not type, error: %v", topic, task.Type(), err)
			return nil
		}
		// 有可能类型不对，值为空值的情况，需要注意
		return handler(ctx, ev)
	})
	if !isNew {
		return fmt.Errorf("topic %s sub already start", topic)
	}

	if !b.serverStarted {
		b.serverStarted = true
		goroutines.GoAsync(func(params ...any) {
			if err := b.subServer.Start(b.mainMux); err != nil {
				fmt.Println("start redis server error:", err)
			}
		})
	}
	return nil
}
