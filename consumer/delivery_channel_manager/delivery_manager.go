package delivery_channel_manager

import (
	"fmt"
	"sync"

	"bitbucket.org/gank-global/eventbus-client/consumer/connection_initializer"
	"github.com/streadway/amqp"
)

type DeliveryChannelManager interface {
	GetDeliveryChan(queue string) <-chan amqp.Delivery
	InitDeliveryChannelForQueue(queue string) error
	Close()
	ReconnectDeliveryChannel() error
	NotifiedConnectionError()
	ConnectionErrorSolved()
	GetConnectionErrorChan() <-chan bool
}

func NewDeliveryChannelManager(initializer connection_initializer.ConnectionInitializer) DeliveryChannelManager {
	return &deliveryChannelManager{
		locker:                sync.Mutex{},
		queueToDeliveryChan:   make(map[string]chan amqp.Delivery),
		doneChan:              make(chan interface{}),
		connectionInitializer: initializer,
		havingConnectionError: false,
	}
}

type deliveryChannelManager struct {
	connectionInitializer connection_initializer.ConnectionInitializer
	locker                sync.Mutex
	queueToDeliveryChan   map[string]chan amqp.Delivery
	doneChan              chan interface{}
	havingConnectionError bool
	connectionErrorChan   chan bool
}

func (d *deliveryChannelManager) GetDeliveryChan(queue string) <-chan amqp.Delivery {
	return d.queueToDeliveryChan[queue]
}

func (d *deliveryChannelManager) InitDeliveryChannelForQueue(queue string) error {
	ampqChannel, err := d.connectionInitializer.GetAMQPChannel()
	if err != nil {
		return err
	}
	_, existed := d.queueToDeliveryChan[queue]
	if !existed {
		d.queueToDeliveryChan[queue] = make(chan amqp.Delivery)
	}
	amqDeliveryChan, err := ampqChannel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("queue consume error: %s", err)
	}
	go func() {
		for {
			select {
			case <-d.doneChan:
				return
			case delivery, open := <-amqDeliveryChan:
				if !open {
					continue
				}
				d.queueToDeliveryChan[queue] <- delivery
			}
		}
	}()
	return nil
}

func (d *deliveryChannelManager) ReconnectDeliveryChannel() error {
	d.Close()
	d.doneChan = make(chan interface{})
	for queue, _ := range d.queueToDeliveryChan {
		if err := d.InitDeliveryChannelForQueue(queue); err != nil {
			return err
		}
	}
	return nil
}

func (d *deliveryChannelManager) Close() {
	close(d.doneChan)
}

func (d *deliveryChannelManager) NotifiedConnectionError() {
	d.locker.Lock()
	defer func() {
		d.locker.Unlock()
	}()
	if d.havingConnectionError {
		return
	}
	d.connectionErrorChan <- true
	d.havingConnectionError = true
}

func (d *deliveryChannelManager) ConnectionErrorSolved() {
	d.locker.Lock()
	defer func() {
		d.locker.Unlock()
	}()
	d.havingConnectionError = false
}

func (d *deliveryChannelManager) GetConnectionErrorChan() <-chan bool {
	return d.connectionErrorChan
}
