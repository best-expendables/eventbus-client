package consumer_manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	eventbusclient "bitbucket.org/snapmartinc/eventbus-client"
	"bitbucket.org/snapmartinc/eventbus-client/consumer/base_consumer"
	"bitbucket.org/snapmartinc/eventbus-client/consumer/delivery_channel_manager"
	"bitbucket.org/snapmartinc/eventbus-client/helper"
	"github.com/streadway/amqp"
)

type Manager interface {
	AssignConsumerToQueue(queueName string, consumer base_consumer.Consumer, replication int)
	StartConsuming() error
	ShutDown()
}

var ErrInvalidJson = errors.New("payload is not a valid json data")

type consumerManager struct {
	deliveryChannelManager delivery_channel_manager.DeliveryChannelManager
	wg                     sync.WaitGroup
	doneChan               chan interface{}
	consumerByQueue        map[string]base_consumer.Consumer
	consumerByQueueCount   map[string]int
}

func NewConsumerManager(deliveryChannelManager delivery_channel_manager.DeliveryChannelManager) Manager {
	return &consumerManager{
		deliveryChannelManager: deliveryChannelManager,
		wg:                     sync.WaitGroup{},
		doneChan:               make(chan interface{}),
		consumerByQueue:        make(map[string]base_consumer.Consumer),
		consumerByQueueCount:   map[string]int{},
	}
}

func (c *consumerManager) AssignConsumerToQueue(queueName string, consumer base_consumer.Consumer, replication int) {
	c.consumerByQueue[queueName] = consumer
	c.consumerByQueueCount[queueName] = replication
}

func (c *consumerManager) StartConsuming() error {
	for queueName, _ := range c.consumerByQueueCount {
		if err := c.startConsumingQueue(queueName); err != nil {
			return err
		}
	}
	return nil
}

func (c *consumerManager) startConsumingQueue(queueName string) error {
	if c.deliveryChannelManager.GetDeliveryChan(queueName) == nil {
		if err := c.deliveryChannelManager.InitDeliveryChannelForQueue(queueName); err != nil {
			return err
		}
	}
	consumerForQueue, ok := c.consumerByQueue[queueName]
	if !ok {
		return fmt.Errorf("there is no consumer_manager for queue: %s", queueName)
	}

	for i := 0; i < c.consumerByQueueCount[queueName]; i++ {
		go func() {
			deliveryChan := c.deliveryChannelManager.GetDeliveryChan(queueName)
			for {
				select {
				case <-c.doneChan:
					return
				case delivery := <-deliveryChan:
					msg := c.processDelivery(queueName, delivery, consumerForQueue)
					for {
						err := c.AckDelivery(delivery, msg.Status)
						if err == nil || err != amqp.ErrClosed {
							break
						}
						c.deliveryChannelManager.NotifiedConnectionError()
						time.Sleep(time.Second * 2)
					}
				}
			}
		}()
	}
	return nil
}

func (c *consumerManager) processDelivery(queueName string, d amqp.Delivery, consumer base_consumer.Consumer) *eventbusclient.Message {
	if !json.Valid(d.Body) {
		return &eventbusclient.Message{
			Status: eventbusclient.MessageStatusAck,
			Error:  ErrInvalidJson,
		}
	}

	msg := helper.GetMessageFromDelivery(d)
	if msg.Error != nil {
		return msg
	}
	c.processMessage(consumer, helper.ContextFromMessage(msg), msg)
	return msg
}

func (c *consumerManager) AckDelivery(delivery amqp.Delivery, status string) error {
	switch status {
	case eventbusclient.MessageStatusAck:
		return delivery.Ack(false)
	case eventbusclient.MessageStatusReject:
		return delivery.Reject(false)
	case eventbusclient.MessageStatusNack:
		return delivery.Nack(false, false)
	default:
		return errors.New("unknown delivery status")
	}
}

func (c *consumerManager) ShutDown() {
	close(c.doneChan)
}

func (c *consumerManager) processMessage(consumer base_consumer.Consumer, ctx context.Context, message *eventbusclient.Message) {
	if len(consumer.Middlewares()) == 0 {
		consumer.Consume(ctx, message)
	}

	h := consumer.Consume
	for i := len(consumer.Middlewares()) - 1; i >= 0; i-- {
		h = consumer.Middlewares()[i](h)
	}
	h(ctx, message)
}
