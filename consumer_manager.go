package eventbusclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"bitbucket.org/snapmartinc/logger"
	"github.com/streadway/amqp"
)

// ConsumerManager manage the consumers
type ConsumerManager interface {
	//Set consumer for queue
	Add(queueName string, consumer Consumer)

	//Start consuming on 1 queue, can have multiple go routines on 1 queue, need to have 1 consumer
	StartConsuming(queueName string) error

	//Stop consuming on all queues
	ShutDown() error

	// Wait all consumer to finish
	Wait()
}

type ErrorHandler func(ctx context.Context, msg *Message, err error)

var ErrInvalidJson = errors.New("payload is not a valid json data")

type consumerManager struct {
	conf *Config
	wg   *sync.WaitGroup

	locker      *sync.Mutex
	conn        *amqp.Connection
	channel     *amqp.Channel
	isConnected bool
	shutdown    bool

	tag             string
	consumingQueues []string
	consumers       map[string]Consumer
}

// NewConsumer Create new consumer, auto load config from system environment
func NewConsumerManager() (ConsumerManager, error) {
	conf := GetAppConfigFromEnv()

	return NewConsumerManagerWithConfig(&conf)
}

//NewConsumerWithConfig Create new consumer with config as a parameter
func NewConsumerManagerWithConfig(config *Config) (ConsumerManager, error) {
	cm := &consumerManager{
		consumers:       make(map[string]Consumer),
		conn:            nil,
		channel:         nil,
		tag:             "",
		conf:            config,
		consumingQueues: []string{},
		wg:              new(sync.WaitGroup),
		locker:          new(sync.Mutex),
	}

	err := cm.connect()
	if err != nil {
		return nil, err
	}

	return cm, nil
}

// We don't need to listening on notify close because when connection being closed
// deliveries will be empty and it will reach recover()
func (cm *consumerManager) connect() error {
	cm.locker.Lock()
	defer cm.locker.Unlock()

	if cm.isConnected {
		return nil
	}
	var err error

	cm.isConnected = false

	cm.conn, err = amqp.Dial(cm.conf.GetURL())
	if err != nil {
		return fmt.Errorf("dial: %s", err)
	}

	cm.channel, err = cm.conn.Channel()
	if err != nil {
		return fmt.Errorf("create channel error: %s", err)
	}

	err = cm.channel.Qos(cm.conf.PrefectCount, 0, false)
	if err != nil {
		return fmt.Errorf("set prefetch count fail: %s", err)
	}
	cm.isConnected = true

	notifyClose := cm.conn.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		closeErr := <-notifyClose
		cm.isConnected = false
		logger.Errorf("connection closed by ", closeErr)
		if cm.shutdown {
			return
		}
		for {
			logger.Info("reconnecting")
			err = cm.connect()
			if err == nil {
				logger.Info("reconnected")
				break
			}
			logger.Infof("reconnect failed, reason: %s", err)
			time.Sleep(retryDelaySeconds * time.Second)
		}
	}()

	return nil
}

func (cm *consumerManager) Add(queueName string, consumer Consumer) {
	cm.consumers[queueName] = consumer
}

func (cm *consumerManager) Wait() {
	cm.wg.Wait()
}

func (cm *consumerManager) StartConsuming(queueName string) error {
	cm.wg.Add(1)
	cm.addConsumingQueue(queueName)

	return cm.startConsumingQueue(queueName)
}

func (cm *consumerManager) addConsumingQueue(queueName string) {
	cm.consumingQueues = append(cm.consumingQueues, queueName)
}

func (cm *consumerManager) startConsumingQueue(queueName string) error {
	consumer, exists := cm.consumers[queueName]
	if !exists {
		return fmt.Errorf("there is no consumer for queue: %s", queueName)
	}

	deliveries, err := cm.channel.Consume(
		queueName,
		cm.tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("queue consume: %s", err)
	}

	go cm.process(queueName, deliveries, consumer)

	return nil
}

func (cm *consumerManager) ShutDown() error {
	cm.shutdown = true
	// will close() the deliveries channel
	if err := cm.channel.Cancel(cm.tag, true); err != nil {
		return fmt.Errorf("consumer cancel failed: %s", err)
	}

	if err := cm.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}
	return nil
}

func (cm *consumerManager) process(queueName string, deliveries <-chan amqp.Delivery, consumer Consumer) {
	defer func() {
		cm.recover(queueName, deliveries, consumer)
	}()

	for d := range deliveries {
		var msg *Message
		var err error
		if !json.Valid(d.Body) {
			cm.handleError(context.Background(), consumer, msg, ErrInvalidJson)
		} else {
			msg, err = getMessageFromDelivery(d)
			if err != nil {
				cm.handleError(context.Background(), consumer, msg, err)
			} else {
				ctx := contextFromMessage(msg)
				err = makeConsumerMiddlewareChain(consumer.Middlewares(), consumer.Consume)(ctx, msg)
				if err != nil {
					cm.handleError(ctx, consumer, msg, err)
				}
			}
		}

		if msg.Status == MessageStatusReject {
			err = d.Reject(false)
		} else {
			err = d.Ack(false)
		}
		if err != nil {
			if shouldRetryConnectOnError(err) {
				_ = cm.retryConsume()
			}

			fields := getLogFieldsFromMessage(err, msg)
			logger.WithFields(fields).Error("MessageAckFailed")
		}
	}
}

func (cm *consumerManager) processMessage(msg *Message) error {
	defer func() {
		cm.recover(queueName, deliveries, consumer)
	}()

	for d := range deliveries {
		var msg *Message
		var err error
		if !json.Valid(d.Body) {
			cm.handleError(context.Background(), consumer, msg, ErrInvalidJson)
		} else {
			msg, err = getMessageFromDelivery(d)
			if err != nil {
				cm.handleError(context.Background(), consumer, msg, err)
			} else {
				ctx := contextFromMessage(msg)
				err = makeConsumerMiddlewareChain(consumer.Middlewares(), consumer.Consume)(ctx, msg)
				if err != nil {
					cm.handleError(ctx, consumer, msg, err)
				}
			}
		}

		if msg.Status == MessageStatusReject {
			err = d.Reject(false)
		} else {
			err = d.Ack(false)
		}
		if err != nil {
			if shouldRetryConnectOnError(err) {
				_ = cm.retryConsume()
			}

			fields := getLogFieldsFromMessage(err, msg)
			logger.WithFields(fields).Error("MessageAckFailed")
		}
	}
}

func (cm *consumerManager) handleError(ctx context.Context, consumer Consumer, msg *Message, err error) {
	if shouldRetryConnectOnError(err) {
		_ = cm.retryConsume()
	}

	for _, errHandler := range consumer.ErrorHandlers() {
		errHandler(ctx, msg, err)
	}
}

func (cm *consumerManager) retryConsume() error {
	err := cm.connect()
	if err != nil {
		return err
	}

	for _, queue := range cm.consumingQueues {
		_ = cm.startConsumingQueue(queue)
	}

	return nil
}

func (cm *consumerManager) recover(queueName string, deliveries <-chan amqp.Delivery, consumer Consumer) {
	if r := recover(); r != nil {
		// recover from panic when consuming message form queue
		logger.Errorf("recovered from: ", r)
		debug.PrintStack()
		go cm.process(queueName, deliveries, consumer)
		return
	}

	if cm.shutdown {
		cm.wg.Done()
		return
	}

	var err error
	for {
		deliveries, err = cm.reInitQueue(queueName)
		if err == nil {
			break
		}
	}

	go cm.process(queueName, deliveries, consumer)
}

func (cm *consumerManager) reInitQueue(queueName string) (<-chan amqp.Delivery, error) {
	err := cm.connect()

	if err != nil {
		logger.Errorf("failed to reconnect: %s", err)
		return nil, err
	}

	return cm.channel.Consume(
		queueName,
		cm.tag,
		false,
		false,
		false,
		false,
		nil,
	)
}
