package flow

import (
	"net/http"

	"github.com/BaritoLog/go-boilerplate/errkit"
	"github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type baritoProducerService struct {
	factory       KafkaFactory
	addr          string
	topicSuffix   string
	newEventTopic string

	producer sarama.SyncProducer
	admin    KafkaAdmin
	server   *http.Server
	limiter  RateLimiter
}

func newBaritoProducerServiceHTTP(factory KafkaFactory, addr string, maxTps int, topicSuffix string, newEventTopic string) BaritoProducerService {
	return &baritoProducerService{
		factory:       factory,
		addr:          addr,
		topicSuffix:   topicSuffix,
		newEventTopic: newEventTopic,
	}
}

func (s *baritoProducerService) Start() (err error) {

	s.producer, err = s.factory.MakeSyncProducer()
	if err != nil {
		err = errkit.Concat(ErrMakeSyncProducer, err)
		return
	}

	s.admin, err = s.factory.MakeKafkaAdmin()
	if err != nil {
		err = errkit.Concat(ErrMakeKafkaAdmin, err)
		return
	}

	s.limiter = NewRateLimiter(timekit.Duration("1s"))
	s.limiter.Start()

	server := s.initHttpServer()
	return server.ListenAndServe()
}

func (s *baritoProducerService) initHttpServer() (server *http.Server) {
	server = &http.Server{
		Addr:    s.addr,
		Handler: s,
	}

	s.server = server
	return
}

func (a *baritoProducerService) Close() {
	if a.server != nil {
		a.server.Close()
	}

	if a.limiter != nil {
		a.limiter.Stop()
	}

	if a.admin != nil {
		a.admin.Close()
	}

	if a.producer != nil {
		a.producer.Close()
	}

}

func (s *baritoProducerService) ServeHTTP(rw http.ResponseWriter, req *http.Request) {

	timber, err := ConvertRequestToTimber(req)
	if err != nil {
		onBadRequest(rw, err)
		return
	}

	// add suffix
	topic := timber.Context().KafkaTopic + s.topicSuffix

	maxTokenIfNotExist := timber.Context().AppMaxTPS
	if s.limiter.IsHitLimit(topic, maxTokenIfNotExist) {
		onLimitExceeded(rw)
		return
	}

	var newTopicCreated bool

	if !s.admin.Exist(topic) {
		numPartitions := timber.Context().KafkaPartition
		replicationFactor := timber.Context().KafkaReplicationFactor

		log.Infof("%s is not exist. Creating topic with partition:%v replication_factor:%v", topic, numPartitions, replicationFactor)

		err = s.admin.CreateTopic(topic, numPartitions, replicationFactor)
		if err != nil {
			onCreateTopicError(rw, err)
			return
		}

		s.admin.AddTopic(topic)
		s.sendCreateTopicEvents(topic)
		newTopicCreated = true
	}

	err = s.sendLogs(topic, timber)
	if err != nil {
		onStoreError(rw, err)
		return
	}

	onSuccess(rw, ProduceResult{
		Topic:      topic,
		IsNewTopic: newTopicCreated,
	})
}

func (s *baritoProducerService) sendLogs(topic string, timber Timber) (err error) {
	message := ConvertTimberToKafkaMessage(timber, topic)
	_, _, err = s.producer.SendMessage(message)
	return
}

func (s *baritoProducerService) sendCreateTopicEvents(topic string) (err error) {
	message := &sarama.ProducerMessage{
		Topic: s.newEventTopic,
		Value: sarama.ByteEncoder(topic),
	}
	_, _, err = s.producer.SendMessage(message)
	return
}
