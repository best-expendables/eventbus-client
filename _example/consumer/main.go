package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	eventbusclient "github.com/best-expendables/eventbus-client"
	"github.com/best-expendables/eventbus-client/consumer/base_consumer"
	"github.com/best-expendables/eventbus-client/consumer/connection_initializer"
	consumer_manager2 "github.com/best-expendables/eventbus-client/consumer/consumer_manager"
	"github.com/best-expendables/eventbus-client/consumer/consumer_middleware"
	"github.com/best-expendables/eventbus-client/consumer/delivery_channel_manager"
	"github.com/best-expendables/eventbus-client/consumer/facade"
	"github.com/best-expendables/logger"
)

type SimpleConsumer struct {
	base_consumer.BaseConsumer
}

func (h *SimpleConsumer) Consume(_ context.Context, message *eventbusclient.Message) {
	//panic(errors.New("test panic recover"))
	message.Error = eventbusclient.NewRetryError(errors.New("test retry error"))
}

func main() {
	config := &eventbusclient.Config{
		Host:         "0.0.0.0",
		Port:         "5673",
		Username:     "guest",
		Password:     "guest",
		PrefectCount: 10,
	}

	//eventBusProducer, err := producer_manager.NewProducerWithConfig(config)
	//if err != nil {
	//	panic(err)
	//}

	connectionInitializer := connection_initializer.NewConnectionInitializer(config)
	deliveryChannelManager := delivery_channel_manager.NewDeliveryChannelManager(connectionInitializer)
	consumerManager := consumer_manager2.NewConsumerManager(deliveryChannelManager)

	f := facade.NewConsumerFacade(
		connectionInitializer,
		deliveryChannelManager,
		consumerManager,
	)
	c := &SimpleConsumer{}
	c.Use(
		consumer_middleware.Recover,
		consumer_middleware.MessageLog,
		consumer_middleware.LogFailedMessage,
		//consumer_middleware.RetryWithError(eventBusProducer, 2),
	)

	f.AddQueueAndConsumer("test_creation", c, 1)
	if err := f.Connect(); err != nil {
		panic(err)
	}
	if err := f.StartConsuming(); err != nil {
		panic(err)
	}
	fmt.Println("Start consuming data")

	go func() {
		time.Sleep(time.Second * 10)
		err := f.ShutDown()
		if err != nil {
			logger.Info(err)
		}
	}()
	f.Wait()

	time.Sleep(time.Second * 5)

}
