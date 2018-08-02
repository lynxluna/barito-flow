package flow

import (
	"github.com/BaritoLog/go-boilerplate/errkit"
)

const (
	ErrMakeSyncProducer = errkit.Error("Make sync producer failed")
)

type Protocol int

const (
	ProtocolHTTP        Protocol = 0
	ProtocolSuperCharge Protocol = 1
)

type BaritoProducerService interface {
	Start() error
	Close()
}

func NewBaritoProducerService(factory KafkaFactory, addr string, maxTps int, topicSuffix, newEventTopic string) BaritoProducerService {
	return NewBaritoProducerServiceWithProtocol(ProtocolHTTP, factory, addr, maxTps, topicSuffix, newEventTopic)
}

func NewBaritoProducerServiceWithProtocol(prot Protocol, factory KafkaFactory, addr string,
	maxTps int, topicSuffix, newEventTopic string) BaritoProducerService {
	if prot == ProtocolSuperCharge {
		return newBaritoProducerServiceKCP(factory, addr, maxTps, topicSuffix, newEventTopic)
	} else {
		return newBaritoProducerServiceHTTP(factory, addr, maxTps, topicSuffix, newEventTopic)
	}
}
