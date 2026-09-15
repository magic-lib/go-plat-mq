package mq

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/magic-lib/go-plat-utils/utils/httputil"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/redis/go-redis/v9"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
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
	PollInterval time.Duration // 结果兜底轮询间隔，<=0 时按 pollInterval() 自动计算

	redisOpt         *asynq.RedisClientOpt
	redisClient      *redis.Client    // 用于 Pub/Sub 实时推送结果（跨进程/分布式可用）
	inspector        *asynq.Inspector // 共享，避免每次 Request 新建 Redis 连接池
	pushClient       *asynq.Client
	subServer        *asynq.Server
	serverStarted    bool
	startTimer       *time.Timer // 兜底延迟启动 timer：未在 Subscribe 后显式 Start 时触发
	mainMux          *asynq.ServeMux
	subscribedTopics cmap.ConcurrentMap[string, bool]
	pushTypeTopics   cmap.ConcurrentMap[string, reflect.Type]
	topicMu          sync.Mutex
	hubMu            sync.Mutex // 保护 hub，避免持 mu 做大延迟的网络 IO
	hub              *resultHub
	mu               sync.RWMutex
	closed           bool
}

// resultPrefix 结果 channel 的公共前缀：mq:result:{namespace}:
func (b *AsynqMessageQueue) resultPrefix() string {
	return fmt.Sprintf("mq:result:%s:", b.Namespace)
}

// resultChannel 返回某个任务结果推送的 Redis channel 名
func (b *AsynqMessageQueue) resultChannel(taskID string) string {
	return b.resultPrefix() + taskID
}

// resultPattern 结果 channel 的订阅模式（供共享 hub 使用 PSubscribe）
func (b *AsynqMessageQueue) resultPattern() string {
	return b.resultPrefix() + "*"
}

// taskIDOf 从结果 channel 名反解出 taskID
func (b *AsynqMessageQueue) taskIDOf(channel string) string {
	return strings.TrimPrefix(channel, b.resultPrefix())
}

// NewAsynqMessageQueue 创建新的 AsynqMessageQueue 实例
func NewAsynqMessageQueue(cfg *conn.Connect, mqConf *AsynqMessageQueue) (*AsynqMessageQueue, error) {
	if cfg == nil || cfg.Host == "" || cfg.Port == "" {
		return nil, fmt.Errorf("redis config error")
	}
	db := 0
	defaultWorkerNum := 30
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
			Concurrency: defaultWorkerNum,
			Queues:      map[string]int{},
		}
	}

	if mqConf.ServerConfig.Queues == nil {
		mqConf.ServerConfig.Queues = make(map[string]int)
	}

	mqConf.pushTypeTopics = cmap.New[reflect.Type]()
	mqConf.subscribedTopics = cmap.New[bool]()
	mqConf.mainMux = asynq.NewServeMux()
	mqConf.inspector = asynq.NewInspector(redisOpt)

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
		asynq.Queue(topicKey),
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
	// 先停结果订阅，唤醒所有等待中的 Request，避免它们继续占用资源
	b.stopResultHub()
	_ = b.pushClient.Close()
	if b.inspector != nil {
		_ = b.inspector.Close()
	}
	if b.redisClient != nil {
		_ = b.redisClient.Close()
	}
	if b.subServer != nil {
		b.subServer.Shutdown()
	}
	if b.startTimer != nil {
		b.startTimer.Stop()
		b.startTimer = nil
	}
}

// Subscribe 实现 Consumer 接口
// 仅注册 handler 与对应 topic 队列，不启动 server；server 需在所有 Subscribe 调用完成后由 Start() 启动。
func (b *AsynqMessageQueue) Subscribe(topic string, handler ConsumerHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("bus is closed")
	}
	topicKey := b.getTopicKey(topic)
	// 每个 topic 独立队列：任务只会出现在「注册了该 topic 的 worker」所监听的队列中，
	// 从根本上避免「共享命名空间队列时，未注册 topic 的任务被其它 worker 抢到 → handler not found」的间歇故障。
	if _, ok := b.ServerConfig.Queues[topicKey]; !ok {
		b.ServerConfig.Queues[topicKey] = 1
	}
	// 兜底：若调用方未在全部 Subscribe 完成后显式调用 Start()（例如单测、单 topic 场景），
	// 则在短暂延迟后自动启动 server，此时所有同步注册的 topic 队列均已就绪。
	// 注意：RegisterActivities 会在循环结束后显式调用 Start()，会立即启动并取消此 timer。
	if !b.serverStarted {
		if b.startTimer != nil {
			b.startTimer.Stop()
		}
		b.startTimer = time.AfterFunc(200*time.Millisecond, func() {
			if err := b.Start(); err != nil {
				log.Println("asynq auto-start server error:", err)
			}
		})
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
		respString := conv.String(resp)
		if rw := task.ResultWriter(); rw != nil {
			_, _ = rw.Write([]byte(respString))
		}
		// 通过 Redis Pub/Sub 实时推送结果，唤醒等待中的 Request（支持分布式）。
		// 即便 ResultWriter 不可用也要推送，否则调用方只能靠轮询兜底才能拿到结果。
		if b.redisClient != nil {
			if pubErr := b.redisClient.Publish(context.Background(), b.resultChannel(ev.Id), respString).Err(); pubErr != nil {
				log.Printf("publish result to redis pub/sub failed for task %s: %v", ev.Id, pubErr)
			}
		}
		// 业务错误不重试：用 asynq.SkipRetry 哨兵错误包装，避免 asynq 默认重试（MaxRetry=25）
		// 业务错误通常是确定性的，重试无意义，且会让 Call 调用方长时间阻塞
		if handlerErr != nil {
			//return fmt.Errorf("%w: %v", asynq.SkipRetry, handlerErr)
			log.Printf("%s: %v", asynq.SkipRetry.Error(), handlerErr)
			return nil
		}
		return nil
	})
	if !isNew {
		return fmt.Errorf("topic %s sub already start", topic)
	}
	return nil
}

// Start 启动消费端 server，消费所有已通过 Subscribe 注册的 topic 队列。
// 必须在完成全部 Subscribe 调用之后调用一次；可重复调用（会用当前队列集合重建 server，
// 以支持注册完成后再动态新增 topic 的场景）。
func (b *AsynqMessageQueue) Start() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.startTimer != nil {
		b.startTimer.Stop()
		b.startTimer = nil
	}
	if b.closed {
		return fmt.Errorf("bus is closed")
	}
	if len(b.ServerConfig.Queues) == 0 {
		return fmt.Errorf("no queue subscribed, call Subscribe before Start")
	}
	if b.subServer != nil {
		b.subServer.Shutdown()
	}
	b.subServer = asynq.NewServer(b.redisOpt, *b.ServerConfig)
	b.serverStarted = true
	goroutines.GoAsync(func(params ...any) {
		if err := b.subServer.Start(b.mainMux); err != nil {
			log.Println("start asynq server error:", err)
		}
	})
	return nil
}

// resultHub 整个实例复用一条 Pub/Sub 连接，按 taskID 在本地分发结果。
// 避免「每次 Request 都新建一条独占连接」导致：连接数随并发线性增长、goroutine 泄漏、
// 以及依赖调用方 Close 才能回收的生命周期问题。
type resultHub struct {
	mu     sync.Mutex
	subs   map[string]chan string // taskID -> 结果通道（缓冲 1，一次性投递）
	ps     *redis.PubSub
	closed bool
}

// startResultHub 懒启动共享订阅（幂等）。失败时不影响调用方，退化为纯轮询兜底。
func (b *AsynqMessageQueue) startResultHub() error {
	b.mu.RLock()
	closed := b.closed
	redisClient := b.redisClient
	b.mu.RUnlock()
	if closed {
		return fmt.Errorf("bus is closed")
	}
	if redisClient == nil {
		return fmt.Errorf("redis client is nil")
	}

	b.hubMu.Lock()
	defer b.hubMu.Unlock()
	if b.hub != nil {
		return nil
	}
	ps := redisClient.PSubscribe(context.Background(), b.resultPattern())
	// Ping 确认订阅已在服务端生效，避免首个结果在订阅生效前被发布而丢失
	if err := ps.Ping(context.Background()); err != nil {
		_ = ps.Close()
		return err
	}
	hub := &resultHub{subs: make(map[string]chan string), ps: ps}
	b.hub = hub
	go hub.dispatch(ps.Channel(), b.taskIDOf)
	return nil
}

// stopResultHub 关闭共享订阅并唤醒所有仍在等待的调用方
func (b *AsynqMessageQueue) stopResultHub() {
	b.hubMu.Lock()
	hub := b.hub
	b.hub = nil
	b.hubMu.Unlock()
	if hub == nil {
		return
	}
	hub.mu.Lock()
	hub.closed = true
	hub.mu.Unlock()
	// Close 会关闭 msgCh，dispatch 退出并 close 掉所有等待中的通道
	_ = hub.ps.Close()
}

// dispatch 收到消息后按 taskID 投递；无人等待则丢弃（该结果仍会被轮询兜底拿到）
func (h *resultHub) dispatch(msgCh <-chan *redis.Message, taskIDOf func(string) string) {
	for msg := range msgCh {
		if msg == nil {
			continue
		}
		taskID := taskIDOf(msg.Channel)
		h.mu.Lock()
		ch, ok := h.subs[taskID]
		if ok {
			delete(h.subs, taskID) // 一次性：投递后即注销，防止重复投递
		}
		h.mu.Unlock()
		if !ok {
			continue // 无人等待（已超时返回），直接丢弃
		}
		select {
		case ch <- msg.Payload:
		default: // 缓冲 1 且非阻塞，绝不卡住分发协程
		}
	}
	// msgCh 已关闭（hub 被 Close）：唤醒所有仍在等待的调用方
	h.mu.Lock()
	for id, ch := range h.subs {
		close(ch)
		delete(h.subs, id)
	}
	h.mu.Unlock()
}

// subscribeResult 登记等待某个 taskID 的结果，返回的 unsub 必须由调用方 defer 执行
func (b *AsynqMessageQueue) subscribeResult(taskID string) (<-chan string, func()) {
	b.hubMu.Lock()
	hub := b.hub
	b.hubMu.Unlock()
	if hub == nil {
		return nil, func() {}
	}
	ch := make(chan string, 1)
	hub.mu.Lock()
	if hub.closed {
		hub.mu.Unlock()
		return nil, func() {}
	}
	hub.subs[taskID] = ch
	hub.mu.Unlock()
	return ch, func() {
		hub.mu.Lock()
		delete(hub.subs, taskID)
		hub.mu.Unlock()
	}
}

// Request 同步提交任务并等待 Consumer 处理完毕，实时返回执行结果
// 类似 HTTP 请求-响应模式，会阻塞直到任务完成或超时，返回 any
//
// 可靠性设计：
//  1. 先在共享 hub 登记订阅，再投递任务 —— Pub/Sub 只投递「订阅生效之后」发布的消息；
//  2. Pub/Sub 仅作低延迟快路径，不保证送达（订阅未生效、连接重连窗口内的消息都会丢），
//     因此主循环持续用 Inspector 周期轮询兜底，任何情况下都不会空等到超时；
//  3. 订阅只是本地 map 登记，不新建 Redis 连接，生命周期由 defer unsub 精确管理。
func (b *AsynqMessageQueue) Request(ctx context.Context, event *Event) (*httputil.CommResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if event == nil {
		return nil, fmt.Errorf("event is empty")
	}
	if event.Id == "" {
		event.Id = uuid.NewString()
	}
	taskID := event.Id

	// 1. 先登记结果订阅（复用共享连接），再投递任务
	if err := b.startResultHub(); err != nil {
		log.Printf("mq: result hub unavailable, fallback to polling only: %v", err)
	}
	resultCh, unsub := b.subscribeResult(taskID)
	defer unsub()

	// 2. 投递任务
	if _, err := b.Publish(ctx, event); err != nil {
		return nil, err
	}

	// 3. 等待结果
	return b.waitResult(ctx, taskID, resultCh)
}

// waitResult Pub/Sub 实时推送（快路径）+ Inspector 周期轮询（正确性兜底）
func (b *AsynqMessageQueue) waitResult(ctx context.Context, taskID string, resultCh <-chan string) (*httputil.CommResponse, error) {
	inspector := b.getInspector()

	deadline := time.Now().Add(b.Timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	ticker := time.NewTicker(b.pollInterval())
	defer ticker.Stop()

	for {
		// 每轮主动查一次：覆盖「结果早于订阅生效已发布」与「Pub/Sub 消息丢失」两种情况
		if inspector != nil {
			if resp, done, err := b.checkTaskResult(inspector, taskID); done {
				return resp, err
			}
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, fmt.Errorf("call: wait result timeout for task %s, timeout: %s", taskID, b.Timeout)
		case payload, ok := <-resultCh: // resultCh 为 nil 时该 case 永久阻塞，不影响其余分支
			if !ok {
				resultCh = nil // hub 已关闭，退化为纯轮询
				continue
			}
			resp := &httputil.CommResponse{}
			if len(payload) > 0 {
				_ = conv.Unmarshal([]byte(payload), resp)
			}
			if resp.Message != "" {
				return resp, fmt.Errorf("%s", resp.Message)
			}
			return resp, nil
		case <-ticker.C:
			// 回到循环顶部再查一次任务终态
		}
	}
}

// pollInterval 兜底轮询间隔：快任务能很快补到结果，慢任务不过度压 Redis
func (b *AsynqMessageQueue) pollInterval() time.Duration {
	if b.PollInterval > 0 {
		return b.PollInterval
	}
	interval := b.Timeout / 50
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	if interval > 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	return interval
}

// getInspector 共享 Inspector，避免每次 Request 新建 Redis 连接池
func (b *AsynqMessageQueue) getInspector() *asynq.Inspector {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inspector == nil && b.redisOpt != nil {
		b.inspector = asynq.NewInspector(*b.redisOpt)
	}
	return b.inspector
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
