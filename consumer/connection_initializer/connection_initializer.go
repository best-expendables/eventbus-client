package connection_initializer

import (
	"errors"
	"fmt"
	"sync"
	"time"

	eventbusclient "bitbucket.org/gank-global/eventbus-client"
	"github.com/best-expendables/logger"
	"github.com/streadway/amqp"
)

const (
	ConnectionManagerStatusNew          = "new"
	ConnectionManagerStatusConnected    = "connected"
	ConnectionManagerStatusDisconnected = "disconnected"
	ConnectionManagerStatusShutdown     = "shutdown"
)

var ConnectionManagerDisconnected = errors.New("connection manager disconnected")

type ConnectionInitializer interface {
	Connect() error
	ShutDown() error
	GetAMQPChannel() (*amqp.Channel, error)
	ReconnectWithConnectionError()
	ReconnectSuccessfulNotifierChannel() <-chan bool
}

type connectionInitializer struct {
	conf   *eventbusclient.Config
	locker sync.Mutex

	conn                        *amqp.Connection
	channel                     *amqp.Channel
	status                      string
	reconnectSuccessfulNotifier chan bool
	doneChan                    chan interface{}
}

func NewConnectionInitializer(conf *eventbusclient.Config) ConnectionInitializer {
	return &connectionInitializer{
		conf:                        conf,
		locker:                      sync.Mutex{},
		conn:                        nil,
		channel:                     nil,
		status:                      ConnectionManagerStatusNew,
		reconnectSuccessfulNotifier: make(chan bool),
		doneChan:                    make(chan interface{}),
	}
}

func (cm *connectionInitializer) Connect() error {
	if cm.status == ConnectionManagerStatusConnected {
		return nil
	}
	var err error

	cm.conn, err = amqp.Dial(cm.conf.GetURL())
	if err != nil {
		return fmt.Errorf("dial: %s", err)
	}

	cm.channel, err = cm.conn.Channel()
	if err != nil {
		return fmt.Errorf("create channel error: %s", err)
	}

	err = cm.channel.Qos(cm.conf.PrefectCount, 0, false)
	if err != nil {
		return fmt.Errorf("set prefetch count fail: %s", err)
	}
	cm.status = ConnectionManagerStatusConnected
	return nil
}

func (cm *connectionInitializer) ShutDown() error {
	close(cm.doneChan)
	if cm.status == ConnectionManagerStatusConnected {
		if err := cm.channel.Cancel("", true); err != nil {
			return fmt.Errorf("consumer_manager cancel failed: %s", err)
		}
		if err := cm.conn.Close(); err != nil {
			return fmt.Errorf("AMQP connection close error: %s", err)
		}
	}
	cm.status = ConnectionManagerStatusShutdown
	cm.channel = nil
	cm.conn = nil
	return nil
}

func (cm *connectionInitializer) ReconnectWithConnectionError() {
	go func() {
		for {
			select {
			case <-cm.doneChan:
				return
			case closeErr := <-cm.conn.NotifyClose(make(chan *amqp.Error)):
				logger.Errorf("connection closed by ", closeErr)

				if cm.status == ConnectionManagerStatusShutdown {
					return
				}
				cm.status = ConnectionManagerStatusDisconnected
				for {
					logger.Info("reconnecting")
					err := cm.Connect()
					if err == nil {
						cm.status = ConnectionManagerStatusConnected
						cm.reconnectSuccessfulNotifier <- true
						logger.Info("reconnected")
						break
					}
					time.Sleep(time.Second)
				}
				go cm.ReconnectWithConnectionError()
				return
			}
		}
	}()
}

func (cm *connectionInitializer) ReconnectSuccessfulNotifierChannel() <-chan bool {
	return cm.reconnectSuccessfulNotifier
}

func (cm *connectionInitializer) GetAMQPChannel() (*amqp.Channel, error) {
	if cm.status != ConnectionManagerStatusConnected {
		return nil, ConnectionManagerDisconnected
	}
	return cm.channel, nil
}
