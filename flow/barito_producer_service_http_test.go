package flow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/BaritoLog/barito-flow/mock"
	. "github.com/BaritoLog/go-boilerplate/testkit"
	"github.com/golang/mock/gomock"
)

func TestBaritoProducerService_ServeHTTP_OnLimitExceed(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	limiter := NewDummyRateLimiter()
	limiter.Expect_IsHitLimit_AlwaysTrue()

	srv := &baritoProducerService{
		limiter: limiter,
	}

	req, _ := http.NewRequest("POST", "/", bytes.NewReader(sampleRawTimber()))
	resp := RecordResponse(srv.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, 509)
}

func TestBaritoProducerService_ServeHTTP_OnBadRequest(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	producer := mock.NewMockSyncProducer(ctrl)
	producer.EXPECT().Close()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Close().AnyTimes()

	agent := &baritoProducerService{
		baritoProducerConfig: baritoProducerConfig{
			topicSuffix: "_logs",
		},
		producer: producer,
		admin:    admin,
	}
	defer agent.Close()

	req, _ := http.NewRequest("POST", "/", strings.NewReader(`invalid-body`))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusBadRequest)
}

func TestBaritoProducerService_ServeHTTP_OnStoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Exist(gomock.Any()).Return(true)

	producer := mock.NewMockSyncProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any()).
		Return(int32(0), int64(0), fmt.Errorf("some-error"))

	limiter := NewDummyRateLimiter()

	agent := &baritoProducerService{
		baritoProducerConfig: baritoProducerConfig{
			topicSuffix: "_logs",
		},

		producer: producer,
		admin:    admin,
		limiter:  limiter,
	}

	req, _ := http.NewRequest("POST", "/", bytes.NewReader(sampleRawTimber()))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusBadGateway)
}

func TestBaritoProducerService_ServeHTTP_OnCreateTopicError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Exist(gomock.Any()).Return(false)
	admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("create-topic-error"))

	producer := mock.NewMockSyncProducer(ctrl)

	limiter := NewDummyRateLimiter()

	agent := &baritoProducerService{
		baritoProducerConfig: baritoProducerConfig{
			topicSuffix: "_logs",
		},

		producer: producer,
		admin:    admin,
		limiter:  limiter,
	}

	req, _ := http.NewRequest("POST", "/", bytes.NewReader(sampleRawTimber()))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusServiceUnavailable)
}

func TestBaritoProducerService_ServeHTTP_OnSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	admin := mock.NewMockKafkaAdmin(ctrl)
	admin.EXPECT().Exist(gomock.Any()).Return(false)
	admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)
	admin.EXPECT().AddTopic(gomock.Any())

	producer := mock.NewMockSyncProducer(ctrl)
	producer.EXPECT().SendMessage(gomock.Any()).AnyTimes()

	limiter := NewDummyRateLimiter()

	agent := &baritoProducerService{
		baritoProducerConfig: baritoProducerConfig{
			topicSuffix: "_logs",
		},

		producer: producer,
		admin:    admin,
		limiter:  limiter,
	}

	req, _ := http.NewRequest("POST", "/", bytes.NewReader(sampleRawTimber()))
	resp := RecordResponse(agent.ServeHTTP, req)

	FatalIfWrongResponseStatus(t, resp, http.StatusOK)

	var result ProduceResult
	b, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(b, &result)

	FatalIf(t, result.Topic != "some_topic_logs", "wrong result.Topic")
	FatalIf(t, result.IsNewTopic != true, "wrong result.IsNewTopic")
}
