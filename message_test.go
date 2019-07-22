package eventbusclient

import (
	"gopkg.in/go-playground/validator.v9"
	"testing"
	"time"
)

func TestValidate(t *testing.T) {
	validate := validator.New()
	message := Message{
		Id:         "Id",
		Exchange:   "exchange_1",
		RoutingKey: "routing_key_1",
		Header: Header{
			Timestamp:   time.Now(),
			Publisher:   "Publisher",
			EventName:   "EventName",
			TraceId:     "TraceId",
			UserId:      "UserId",
			XRetryCount: 12,
		},
		Payload: Payload{
			EntityId: "EntityId",
			Data:     "Data",
		},
	}

	err := validate.Struct(message)

	if err != nil {
		t.Errorf("There should be no error")
	}
}

func TestValidateHasError(t *testing.T) {
	validate := validator.New()
	message := Message{
		Id:         "Id",
		Exchange:   "exchange_1",
		RoutingKey: "routing_key_1",
		Header: Header{
			Publisher: "Publisher",
			EventName: "EventName",
			TraceId:   "TraceId",
			UserId:    "UserId",
		},
		Payload: Payload{
			EntityId: "EntityId",
			Data:     "Data",
		},
	}

	err := validate.Struct(message)

	if err == nil {
		t.Error("There should be error")
	}
}

func TestHeader_FromMap(t *testing.T) {
	var cases = []struct {
		desc      string
		headerMap map[string]interface{}
		expect    Header
	}{
		{
			desc: "with retry count",
			headerMap: map[string]interface{}{
				"timestamp":   time.Now().Format("20060102150405"),
				"publisher":   "package",
				"eventName":   "package_creation",
				"traceId":     "trace_id_here",
				"userId":      "user_id_here",
				"xRetryCount": 199,
			},
			expect: Header{
				Publisher:   "package",
				EventName:   "package_creation",
				TraceId:     "trace_id_here",
				UserId:      "user_id_here",
				XRetryCount: 199,
			},
		},
		{
			desc: "without retry count",
			headerMap: map[string]interface{}{
				"timestamp": time.Now().Format("20060102150405"),
				"publisher": "package",
				"eventName": "package_creation",
				"traceId":   "trace_id_here",
				"userId":    "user_id_here",
			},
			expect: Header{
				Publisher:   "package",
				EventName:   "package_creation",
				TraceId:     "trace_id_here",
				UserId:      "user_id_here",
				XRetryCount: 0,
			},
		},
	}

	for _, c := range cases {
		h := &Header{}
		err := h.FromMap(c.headerMap)
		if err != nil {
			t.Errorf("fail case: %s", c.desc)
		}
		if h.XRetryCount != c.expect.XRetryCount || h.EventName != c.expect.EventName {
			t.Errorf("fail case: %s", c.desc)
		}
	}
}

func TestHeader_ToMap(t *testing.T) {
	timeToCheck := time.Now()
	var cases = []struct {
		desc      string
		headerMap map[string]interface{}
		header    Header
	}{
		{
			desc: "with retry count",
			headerMap: map[string]interface{}{
				"timestamp":   timeToCheck.Unix(),
				"publisher":   "package",
				"eventName":   "package_creation",
				"traceId":     "trace_id_here",
				"userId":      "user_id_here",
				"xRetryCount": 199,
			},
			header: Header{
				Timestamp:   timeToCheck,
				Publisher:   "package",
				EventName:   "package_creation",
				TraceId:     "trace_id_here",
				UserId:      "user_id_here",
				XRetryCount: 199,
			},
		},
		{
			desc: "without retry count",
			headerMap: map[string]interface{}{
				"timestamp": timeToCheck.Unix(),
				"publisher": "package",
				"eventName": "package_creation",
				"traceId":   "trace_id_here",
				"userId":    "user_id_here",
			},
			header: Header{
				Timestamp:   timeToCheck,
				Publisher:   "package",
				EventName:   "package_creation",
				TraceId:     "trace_id_here",
				UserId:      "user_id_here",
				XRetryCount: 0,
			},
		},
	}

	for _, c := range cases {
		for key, val := range c.headerMap {
			resultMap := c.header.ToMap()
			if key != "xRetryCount" {
				if resultMap[key] != val {
					t.Errorf("fail case: %s, fields: %s, got: %v - expect: %v", c.desc, key, resultMap[key], val)
				}
			}

		}
	}
}
