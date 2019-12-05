package facade

import (
	"time"

	"bitbucket.org/snapmartinc/eventbus-client/consumer/base_consumer"
	"bitbucket.org/snapmartinc/eventbus-client/consumer/connection_initializer"
	"bitbucket.org/snapmartinc/eventbus-client/consumer/consumer_manager"
	"bitbucket.org/snapmartinc/eventbus-client/consumer/delivery_channel_manager"
	"bitbucket.org/snapmartinc/logger"
)

type ConsumerFacade interface {
	AddQueueAndConsumer(queueName string, consumer base_consumer.Consumer, replication int)
	Connect() error
	StartConsuming() error
	ShutDown() error
	Wait()
}

type consumerFacade struct {
	connectionInitializer  connection_initializer.ConnectionInitializer
	consumerManager        consumer_manager.Manager
	deliveryChannelManager delivery_channel_manager.DeliveryChannelManager

	doneChan     chan interface{}
	reconnecting bool
}

func NewConsumerFacade(
	connectionInitializer connection_initializer.ConnectionInitializer,
	deliveryChannelManager delivery_channel_manager.DeliveryChannelManager,
	consumerManager consumer_manager.Manager,
) ConsumerFacade {
	return &consumerFacade{
		connectionInitializer:  connectionInitializer,
		consumerManager:        consumerManager,
		deliveryChannelManager: deliveryChannelManager,
		doneChan:               make(chan interface{}),
		reconnecting:           false,
	}
}

func (c *consumerFacade) AddQueueAndConsumer(queueName string, consumer base_consumer.Consumer, replication int) {
	c.consumerManager.AssignConsumerToQueue(queueName, consumer, replication)
}

func (c *consumerFacade) Connect() error {
	if err := c.connectionInitializer.Connect(); err != nil {
		return err
	}
	c.connectionInitializer.ReconnectWithConnectionError()
	go func() {
		connectionNotifierChan := c.connectionInitializer.ReconnectSuccessfulNotifierChannel()
		c.regainConnection(connectionNotifierChan)
	}()
	return nil
}

func (c *consumerFacade) StartConsuming() error {
	if err := c.consumerManager.StartConsuming(); err != nil {
		return err
	}
	go func() {
		errorWhileComsuming := c.deliveryChannelManager.GetConnectionErrorChan()
		c.regainConnection(errorWhileComsuming)
	}()
	return nil
}

func (c *consumerFacade) regainConnection(notifierChan <-chan bool) {
	for {
		select {
		case <-c.doneChan:
			return
		case <-notifierChan:
			if !c.reconnecting {
				c.reconnecting = true
				connectionRegained := false
				for {
					if !connectionRegained {
						if err := c.connectionInitializer.Connect(); err != nil {
							continue
						}
					}
					connectionRegained = true
					if err := c.deliveryChannelManager.ReconnectDeliveryChannel(); err == nil {
						c.deliveryChannelManager.ConnectionErrorSolved()
						c.reconnecting = false
						break
					}
					time.Sleep(time.Second)
				}
			}
		}
	}
}

func (c *consumerFacade) ShutDown() error {
	c.consumerManager.ShutDown()

	c.deliveryChannelManager.Close()

	if err := c.connectionInitializer.ShutDown(); err != nil {
		logger.Infof("connection initializer failed to disconnect, reason: %s", err)
	}
	close(c.doneChan)
	return nil
}

func (c *consumerFacade) Wait() {
	<-c.doneChan
}
