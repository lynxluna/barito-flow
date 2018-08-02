package flow

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	kcp "github.com/xtaci/kcp-go"
	"github.com/xtaci/smux"
	emoji "gopkg.in/kyokomi/emoji.v1"
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

	listener *kcp.Listener
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

	msg := emoji.Sprint("Starting producer using Supercharged protocol :rocket::rocket::rocket::muscle: ..")
	log.Info(msg)

	listener, err := kcp.ListenWithOptions(s.config.addr, nil, 0, 0)

	if err != nil {
		return err
	}

	s.listener = listener

	for {
		if conn, err := s.listener.AcceptKCP(); err == nil {
			go s.handleMux(conn)
		} else {
			log.Errorf("Error %+v", err)
		}
	}
}

func (s *baritoProducerServiceKCP) Close() {
}

func (s *baritoProducerServiceKCP) handleMux(conn io.ReadWriteCloser) {
	smuxCfg := smux.DefaultConfig()
	smuxCfg.MaxReceiveBuffer = s.config.SockBuf
	smuxCfg.KeepAliveInterval = time.Duration(s.config.KeepAlive) * time.Second

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
		go s.handleClient(stream)
	}
}

func (s *baritoProducerServiceKCP) decodePacket(r io.Reader) ([]byte, error) {
	var header KCPPacketHeader
	var headerBytes [48]byte

	n, err := r.Read(headerBytes[0:])

	if err != nil {
		return nil, err
	}

	if n < len(headerBytes) {
		return nil, errors.New("Error reading header")
	}

	err = header.UnmarshalBinary(headerBytes[0:])

	if err != nil {
		return nil, err
	}

	buf := make([]byte, header.Length)

	n, err = r.Read(buf)

	if err != nil {
		return nil, err
	}

	if uint32(n) < header.Length {
		return nil, fmt.Errorf("Length doesn't equal [header %d != payload %d]", header.Length, n)
	}

	return buf, nil
}

func (s *baritoProducerServiceKCP) handleClient(stream io.ReadCloser) {
	defer stream.Close()

	payload, err := s.decodePacket(stream)

	if nil != err {
		log.Errorf("Error decoding packet!")
		return
	}

	fmt.Printf("Received: '%s\n'", string(payload))
}
