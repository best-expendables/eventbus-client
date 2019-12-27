package helper

import (
	"testing"

	"github.com/streadway/amqp"
)

func TestGetMessageFromDelivery(t *testing.T) {
	header := amqp.Table{
		"eventName":   "package_status_update_to_package_file_batch",
		"publisher":   "package",
		"timestamp":   1551356656,
		"traceId":     `{"v":[0,1],"d":{"ty":"App","ap":"58007583","ac":"1390351","tk":"851197","tx":"f6f817f7ca3126f8","tr":"320976e01868e13f","pr":0.82684237,"sa":false,"ti":1551356656078}}`,
		"userId":      "",
		"xRetryCount": 3,
	}

	delivery := amqp.Delivery{
		Body:    []byte("{}"),
		Headers: header,
	}

	msg, err := GetMessageFromDelivery(delivery)

	if err != nil {
		t.Error("should get no error")
	}
	if msg.Header.XRetryCount != 3 {
		t.Error("wrong retry count")
	}
}
