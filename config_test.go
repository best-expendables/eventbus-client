package eventbusclient_test

import (
	"testing"

	eventbusclient2 "bitbucket.org/snapmartinc/eventbus-client"
)

func TestGetUrl(t *testing.T) {
	conf := eventbusclient2.Config{
		Host:     "127.0.0.1",
		Port:     "5672",
		Username: "guest",
		Password: "QN{jaBV'~J!5b9^+",
	}

	expect := "amqp://guest:QN%7BjaBV%27~J%215b9%5E%2B@127.0.0.1:5672/"

	if conf.GetURL() != expect {
		t.Error("expect get correct connection string")
	}
}
