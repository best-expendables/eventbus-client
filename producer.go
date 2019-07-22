package eventbusclient

import (
	"bitbucket.org/snapmartinc/lgo/logger"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"gopkg.in/go-playground/validator.v9"
	"sync"
	"time"
)

var (
	//ErrMessageSendConfirmFailed confirm message send failed, producer's connection is automatically refreshed
	ErrMessageSendConfirmFailed = errors.New("cannot get confirm, refreshed producer")
	//ErrMessageNotAcked message is not acked by rabbitmq
	ErrMessageNotAcked = errors.New("RMQ did not ack the message")
)

const (
	retryDelaySeconds = 2
)

// Producer set middlewares and Publish message to eventbus
type Producer interface {
	Use(middleware ...PublishFuncMiddleware)
	Publish(ctx context.Context, message *Message) error
	PublishRaw(ctx context.Context, message *Message) error
	Close() error
}

type producer struct {
	url         string
	con         *amqp.Connection
	channel     *amqp.Channel
	confirm     chan amqp.Confirmation
	validate    *validator.Validate
	middlewares []PublishFuncMiddleware
	locker      *sync.Mutex
	closed      bool
	isConnected bool
}

// NewProducer create new producer, autoload config data from system environment
func NewProducer() (Producer, error) {
	conf := GetAppConfigFromEnv()

	return NewProducerWithConfig(&conf)
}

func NewProducerWithConfig(config *Config) (Producer, error) {
	producer := &producer{
		url:      config.GetURL(),
		validate: validator.New(),
		locker:   new(sync.Mutex),
	}
	producer.Use(NewRelicPublishMiddleware)
	producer.Use(PublishMessageLogMiddleware)

	if err := producer.createConnection(); err != nil {
		return nil, err
	}

	return producer, nil
}

func (p *producer) Use(middleWares ...PublishFuncMiddleware) {
	p.middlewares = append(p.middlewares, middleWares...)
}

func (p *producer) createConnection() error {
	p.locker.Lock()
	defer p.locker.Unlock()

	if p.isConnected {
		return nil
	}

	var err error
	if p.con != nil {
		_ = p.con.Close()
	}
	p.con, err = amqp.Dial(p.url)
	if err != nil {
		return err
	}

	p.channel, err = p.con.Channel()
	if err != nil {
		return err
	}

	if err := p.channel.Confirm(false); err != nil {
		return fmt.Errorf("channel could not be put into confirm mode: %s", err)
	}

	p.confirm = p.channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	notifyClose := p.con.NotifyClose(make(chan *amqp.Error, 1))
	go func(closeChannel chan *amqp.Error) {
		closeErr := <-closeChannel
		p.isConnected = false
		logger.Errorf("connection closed by ", closeErr)
		if p.closed {
			return
		}
		for {
			logger.Info("reconnecting")
			err = p.createConnection()
			if err == nil {
				logger.Info("reconnected")
				break
			}
			logger.Infof("reconnect failed, reason: %s", err)
			time.Sleep(retryDelaySeconds * time.Second)
		}
	}(notifyClose)
	p.isConnected = true

	return nil
}

func (p *producer) Publish(ctx context.Context, msg *Message) error {
	if err := p.validate.Struct(*msg); err != nil {
		return err
	}

	err := makePublisherMiddlewareChain(p.middlewares, p.publish)(ctx, msg)
	if shouldRetryConnectOnError(err) {
		_ = p.createConnection()
		err = p.publish(ctx, msg)
	}

	return err
}

func (p *producer) publish(_ context.Context, msg *Message) error {
	body, err := json.Marshal(msg.Payload)
	if err != nil {
		return err
	}

	publishing := amqp.Publishing{
		MessageId:    msg.Id,
		Headers:      amqp.Table(msg.Header.ToMap()),
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}

	err = p.publishWithConfirm(msg.Exchange, msg.RoutingKey, publishing)
	for err != nil {
		if err == amqp.ErrClosed {
			_ = p.createConnection()
		} else if err == ErrMessageSendConfirmFailed {
			_ = p.createConnection()
		} else if err == ErrMessageNotAcked {
		}
		err = p.publishWithConfirm(msg.Exchange, msg.RoutingKey, publishing)
	}

	return nil
}

func (p *producer) PublishRaw(ctx context.Context, msg *Message) error {

	return makePublisherMiddlewareChain(p.middlewares, p.publishRaw)(ctx, msg)
}

func (p *producer) publishRaw(ctx context.Context, msg *Message) error {
	body, err := json.Marshal(msg.Payload.Data)
	if err != nil {
		return err
	}

	publishing := amqp.Publishing{
		MessageId:    msg.Id,
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}

	err = p.publishWithConfirm(msg.Exchange, msg.RoutingKey, publishing)
	for err != nil {
		if err == amqp.ErrClosed {
			_ = p.createConnection()
		} else if err == ErrMessageSendConfirmFailed {
			_ = p.createConnection()
		} else if err == ErrMessageNotAcked {
		}
		err = p.publishWithConfirm(msg.Exchange, msg.RoutingKey, publishing)
	}

	return nil
}

func (p *producer) publishWithConfirm(exchange, routingKey string, msg amqp.Publishing) error {
	if err := p.channel.Publish(exchange, routingKey, true, false, msg); err != nil {
		return err
	}
	select {
	case confirmed, ok := <-p.confirm:
		if !ok {
			return ErrMessageSendConfirmFailed
		}
		if !confirmed.Ack {
			return ErrMessageNotAcked
		}
		return nil
	}
}

func (p *producer) Close() error {
	p.closed = true
	if err := p.channel.Close(); err != nil {
		return fmt.Errorf("close publisher channel fails: %s", err)
	}

	if err := p.con.Close(); err != nil {
		return fmt.Errorf("close publisher connection fails: %s", err)
	}

	return nil
}

func shouldRetryConnectOnError(err error) bool {
	return err == amqp.ErrClosed
}
