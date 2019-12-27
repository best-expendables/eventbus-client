package eventbusclient

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

type Header struct {
	Timestamp   time.Time `json:"timestamp" validate:"required"`
	Publisher   string    `json:"publisher" validate:"required"`
	EventName   string    `json:"eventName" validate:"required"`
	TraceId     string    `json:"traceId"`
	UserId      string    `json:"userId"`
	XRetryCount int16     `json:"xRetryCount,omitempty"`
}

func (h *Header) FromMap(headers map[string]interface{}) error {
	timestampVal, ok := headers["timestamp"]
	if !ok {
		return errors.New("missing `timestamp` field on header")
	}

	timestamp, err := strconv.ParseFloat(fmt.Sprintf("%v", timestampVal), 64)
	if err != nil {
		return err
	}

	h.Timestamp = time.Unix(int64(timestamp), 0)

	mapMessageHeader(headers, "publisher", &h.Publisher)
	mapMessageHeader(headers, "eventName", &h.EventName)
	mapMessageHeader(headers, "traceId", &h.TraceId)
	mapMessageHeader(headers, "userId", &h.UserId)

	retryInt, ok := headers["xRetryCount"].(int16)
	if ok {
		h.XRetryCount = retryInt
	} else {
		h.XRetryCount = 0
	}

	return nil
}

// ToMap return map of string data from header
func (h *Header) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"timestamp":   h.Timestamp.Unix(),
		"publisher":   h.Publisher,
		"eventName":   h.EventName,
		"traceId":     h.TraceId,
		"userId":      h.UserId,
		"xRetryCount": h.XRetryCount,
	}
}
