package flow

import (
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/golang/mock/gomock"

	"net/http"
	"testing"
)

func TestBaritoProducerService_Start_ErrorMakeSyncProducer(t *testing.T) {
	factory := NewDummyKafkaFactory()
	factory.Expect_MakeSyncProducerFunc_AlwaysError("some-error")

	service := NewBaritoProducerService(factory, "addr", 1, "_logs", "new_topic_events")
	err := service.Start()

	FatalIfWrongError(t, err, "Make sync producer failed: some-error")
}

func TestBaritoProducerService_Start_ErrorMakeKafkaAdmin(t *testing.T) {
	factory := NewDummyKafkaFactory()
	factory.Expect_MakeKafkaAdmin_AlwaysError("some-error")

	service := NewBaritoProducerService(factory, "addr", 1, "_logs", "new_topic_events")
	err := service.Start()

	FatalIfWrongError(t, err, "Make kafka admin failed: some-error")
}

func TestBaritoProducerService_Start(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	factory := NewDummyKafkaFactory()
	factory.Expect_MakeKafkaAdmin_ProducerServiceSuccess(ctrl, []string{})

	service := &baritoProducerService{
		baritoProducerConfig: baritoProducerConfig{
			topicSuffix:   "_logs",
			addr:          ":24400",
			newEventTopic: "new_topic_event",
		},
		factory: factory,
	}

	var err error
	go func() {
		err = service.Start()
	}()
	defer service.Close()

	FatalIfError(t, err)

	timekit.Sleep("1ms")
	FatalIf(t, !service.limiter.IsStart(), "rate limiter must be start")

	resp, err := http.Get("http://:24400")
	FatalIfError(t, err)
	FatalIfWrongResponseStatus(t, resp, http.StatusBadRequest)
}
