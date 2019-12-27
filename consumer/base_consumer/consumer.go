package base_consumer

import (
	"context"

	eventbusclient "bitbucket.org/snapmartinc/eventbus-client"
	"bitbucket.org/snapmartinc/eventbus-client/consumer/consumer_middleware"
)

// Handler handle message received
type Consumer interface {
	// Consumer Message, return error in case of failure
	Consume(ctx context.Context, message *eventbusclient.Message)

	//Specify some midldewares to be use before consuming message
	Use(middleware ...consumer_middleware.Middleware)

	//Return list of middlewares being used
	Middlewares() []consumer_middleware.Middleware
}
