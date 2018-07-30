package flow

import (
	"github.com/BaritoLog/go-boilerplate/errkit"
)

const (
	ErrMakeSyncProducer = errkit.Error("Make sync producer failed")
)

type BaritoProducerService interface {
	Start() error
	Close()
}

func NewBaritoProducerService(factory KafkaFactory, addr string, maxTps int, topicSuffix, newEventTopic string) BaritoProducerService {
	return newBaritoProducerServiceHTTP(factory, addr, maxTps, topicSuffix, newEventTopic)
}
