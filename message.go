package eventbusclient

import (
	"fmt"
)

const (
	MessageStatusAck    = "ack"
	MessageStatusReject = "reject"
	MessageStatusNack   = "nack"
)

type (
	// Message message send to eventbus
	Message struct {
		Id         string
		Exchange   string
		RoutingKey string
		Header     Header
		Payload    Payload `validate:"required,dive"`
		Status     string
		Error      error
	}

	// Payload message's data
	Payload struct {
		EntityId string      `json:"entityId"`
		Data     interface{} `json:"data" validate:"required"`
	}
)

func mapMessageHeader(headers map[string]interface{}, key string, header *string) {
	value, exists := headers[key]
	if !exists {
		value = ""
	}
	*header = fmt.Sprintf("%v", value)
}
