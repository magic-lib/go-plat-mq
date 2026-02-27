package nats

import (
	"context"
	"fmt"
	"github.com/magic-lib/go-plat-utils/conn"
	"github.com/nats-io/nats.go"
	"log"
	"strings"
)

type MqServer struct {
	conn        *nats.Conn
	asyncSubs   map[string]*nats.Subscription
	requestSubs map[string]*nats.Subscription
	subHandler  map[string]nats.MsgHandler
}

func New(cfgList ...*conn.Connect) (*MqServer, error) {
	if len(cfgList) == 0 {
		return nil, fmt.Errorf("nats config is nil")
	}
	servers := make([]string, 0)
	for _, cfg := range cfgList {
		if cfg.Protocol == "" {
			cfg.Protocol = "nats"
		}
		if cfg.Host == "" {
			cfg.Host = "127.0.0.1"
		}
		if cfg.Port == "" {
			cfg.Port = fmt.Sprintf("%d", nats.DefaultPort)
		}
		connUrl := fmt.Sprintf("%s://%s:%s", cfg.Protocol, cfg.Host, cfg.Port)
		servers = append(servers, connUrl)
	}

	nc, err := nats.Connect(strings.Join(servers, ", "))
	if err != nil {
		return nil, err
	}
	return &MqServer{
		conn:        nc,
		asyncSubs:   make(map[string]*nats.Subscription),
		requestSubs: make(map[string]*nats.Subscription),
		subHandler:  make(map[string]nats.MsgHandler),
	}, nil
}
func (m *MqServer) Close() error {
	if m.conn != nil {
		err := m.conn.Drain()
		if err != nil {
			return err
		}
		m.conn = nil
		return nil
	}
	return nil
}

func (m *MqServer) AsyncSubscribe(topic string, handler nats.MsgHandler) error {
	if m.conn == nil {
		return fmt.Errorf("not connect")
	}
	if m.asyncSubs[topic] != nil {
		return fmt.Errorf("topic %s already subscribe", topic)
	}
	subData, err := m.conn.Subscribe(topic, handler)
	if err != nil {
		return err
	}
	m.asyncSubs[topic] = subData
	m.subHandler[topic] = handler
	return nil
}
func (m *MqServer) AsyncQueueSubscribe(topic string, handler nats.MsgHandler) error {
	if m.conn == nil {
		return fmt.Errorf("not connect")
	}
	if m.asyncSubs[topic] != nil {
		return fmt.Errorf("topic %s already subscribe", topic)
	}
	subData, err := m.conn.Subscribe(topic, handler)
	if err != nil {
		return err
	}
	m.asyncSubs[topic] = subData
	m.subHandler[topic] = handler
	return nil
}
func (m *MqServer) Request(ctx context.Context, topic string, reqData *nats.Msg, handler func(msg *nats.Msg) ([]byte, error)) (*nats.Msg, error) {
	if m.conn == nil {
		return nil, fmt.Errorf("not connect")
	}
	if m.requestSubs[topic] == nil {
		subData, err := m.conn.Subscribe(topic, func(msg *nats.Msg) {
			log.Printf("收到消息：%s", msg.Data)
			respData, err := handler(msg)
			if err != nil {
				log.Println(err)
				return
			}
			_ = m.conn.Publish(msg.Reply, respData)
		})
		if err != nil {
			return nil, err
		}
		m.requestSubs[topic] = subData
	}
	reqData.Subject = topic
	return m.conn.RequestMsgWithContext(ctx, reqData)
}
func (m *MqServer) Publish(topic string, msg *nats.Msg) error {
	if m.conn == nil {
		return fmt.Errorf("not connect")
	}
	if msg == nil {
		return nil
	}
	msg.Subject = topic
	return m.conn.PublishMsg(msg)
}
