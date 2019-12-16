package main

import (
	"context"
	"errors"
	"fmt"

	eventbusclient "bitbucket.org/snapmartinc/eventbus-client"
)

type SimpleConsumer struct {
	eventbusclient.BaseConsumer
	b int
}

func (h *SimpleConsumer) Consume(_ context.Context, message *eventbusclient.Message) error {
	h.b++
	fmt.Println(h.b, "++++++++")
	if h.b == 2 {
		return nil
	}
	return eventbusclient.NewRetryError(errors.New("test retry error"))
}

func main() {
	config := eventbusclient.Config{
		Host:         "0.0.0.0",
		Port:         "5673",
		Username:     "guest",
		Password:     "guest",
		PrefectCount: 10,
	}

	eventBusProducer, err := eventbusclient.NewProducerWithConfig(&config)
	if err != nil {
		panic(err)
	}

	fmt.Println("Create consumer")
	consumerManager, err := eventbusclient.NewConsumerManagerWithConfig(&config)
	if err != nil {
		panic(err)
	}

	consumer := &SimpleConsumer{
		b: 0,
	}
	consumer.Use(
		eventbusclient.RecoverWithRetry,
		eventbusclient.MessageLog,
	)
	consumer.UseErrorHandler(
		eventbusclient.LogFailedMessage,
		eventbusclient.RetryWithError(eventBusProducer, 0),
	)

	fmt.Println("Start consuming data")
	queueName := "test_queue_creation"
	consumerManager.Add(queueName, consumer)
	err = consumerManager.StartConsuming(queueName)
	if err != nil {
		panic(err)
	}

	consumerManager.Wait()
}
