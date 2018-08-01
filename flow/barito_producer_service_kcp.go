package flow

import (
	// kcp "github.com/xtaci/kcp-go"
	"github.com/xtaci/smux"

	// "github.com/BaritoLog/go-boilerplate/errkit"
	// "github.com/BaritoLog/go-boilerplate/timekit"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"

	"bytes"
	"errors"
	"io"
	"time"
)

type baritoKCPConfig struct {
	baritoProducerConfig
	MTU       int
	KeepAlive int
	SockBuf   int // Buffer max: 0x400000
}

type baritoProducerServiceKCP struct {
	config baritoKCPConfig

	factory  KafkaFactory
	producer sarama.SyncProducer
	admin    KafkaAdmin
	limiter  RateLimiter
}

func newBaritoProducerServiceKCP(factory KafkaFactory, addr string, maxTps int, topicSuffix string, newEventTopic string) BaritoProducerService {
	return &baritoProducerServiceKCP{
		factory: factory,
		config: baritoKCPConfig{
			baritoProducerConfig: baritoProducerConfig{
				addr:          addr,
				topicSuffix:   topicSuffix,
				newEventTopic: newEventTopic,
			},
		},
	}
}

func (s *baritoProducerServiceKCP) Start() error {
	return errors.New("Not implemented")
}

func (s *baritoProducerServiceKCP) Close() {
}

func decodePacket(r io.Reader, w io.Writer) error {
	return errors.New("Not implemented")
}

func handleClient(stream io.ReadCloser) {
	defer stream.Close()

	var buf bytes.Buffer
	err := decodePacket(stream, &buf)

	if nil != err {
		log.Errorf("Error decoding packet!")
		return
	}

	timber, err := ConvertBytesToTimber(buf.Bytes())
	_ = timber.Context().AppMaxTPS

	/* Send Timber Referring to this code :
	topic := timber.Context().KafkaTopic + suffix

	maxTokenIfNotExist := timber.Context().AppMaxTPS

	if s.limiter.IsHitLimit(topic, maxTokenIfNotExist) {
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
	*/
}

func handleMux(conn io.ReadWriteCloser, cfg *baritoKCPConfig) {
	smuxCfg := smux.DefaultConfig()
	smuxCfg.MaxReceiveBuffer = cfg.SockBuf
	smuxCfg.KeepAliveInterval = time.Duration(cfg.KeepAlive) * time.Second

	mux, err := smux.Server(conn, smuxCfg)

	if nil != err {
		log.Errorf("%s\n", err.Error())
		return
	}

	defer mux.Close()

	for {
		stream, err := mux.AcceptStream()
		if nil != err {
			log.Errorf("%s\n", err.Error())
			return
		}
		go handleClient(stream)
	}
}
