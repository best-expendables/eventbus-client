package main

import (
	"context"
	"log"
	"time"

	eventbusclient "bitbucket.org/gank-global/eventbus-client"
	"bitbucket.org/gank-global/eventbus-client/producer_manager"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func setup(config eventbusclient.Config) {
	conn, err := amqp.Dial(config.GetURL())
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	_, err = ch.QueueDeclare(
		"routing_key_1", // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a queue")

	_, err = ch.QueueDeclare(
		"routing_key_2", // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a queue")
}

func main() {
	config := eventbusclient.Config{
		Host:     "127.0.0.1",
		Port:     "5672",
		Username: "rabbitmq",
		Password: "rabbitmq",
	}
	setup(config)

	producer, err := producer_manager.NewProducerWithConfig(&config)
	if err != nil {
		panic(err)
	}

	message := eventbusclient.Message{
		Id:         "MessageId",
		Exchange:   "",
		RoutingKey: "routing_key_1",
		Header: eventbusclient.Header{
			Timestamp: time.Now(),
			Publisher: "Publisher",
			EventName: "EventName",
			TraceId:   "TraceId",
			UserId:    "UserId",
		},
		Payload: eventbusclient.Payload{
			EntityId: "EntityId",
			Data:     "Data",
		},
	}

	for {
		err = producer.Publish(context.Background(), &message)
		if err != nil {
			panic(err)
		}
	}
}
