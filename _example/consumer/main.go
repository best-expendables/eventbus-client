package main

import (
	"bitbucket.org/snapmartinc/ims/eventbus-client"
	"context"
	"fmt"
)

type SimpleConsumer struct {
	eventbusclient.BaseConsumer
}

func (h SimpleConsumer) Consume(_ context.Context, message *eventbusclient.Message) error {
	fmt.Println("Message data: ", message)
	return nil
}

func main() {
	config := eventbusclient.Config{
		Host:     "127.0.0.1",
		Port:     "5672",
		Username: "rabbitmq",
		Password: "rabbitmq",
	}

	fmt.Println("Create consumer")
	consumerManager, err := eventbusclient.NewConsumerManagerWithConfig(&config)
	if err != nil {
		panic(err)
	}
	consumer := &SimpleConsumer{}
	consumer.Use(
		eventbusclient.MessageLog,
		eventbusclient.Recover,
	)
	consumer.UseErrorHandler(
		eventbusclient.LogFailedMessage,
		eventbusclient.RejectMessage,
	)

	fmt.Println("Start consuming data")
	queueName := "routing_key_1"
	consumerManager.Add(queueName, consumer)
	err = consumerManager.StartConsuming(queueName)
	if err != nil {
		panic(err)
	}

	consumerManager.Wait()
}
