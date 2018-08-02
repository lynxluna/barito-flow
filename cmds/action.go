package cmds

import (
	"github.com/BaritoLog/barito-flow/flow"
	"github.com/BaritoLog/go-boilerplate/srvkit"
	"github.com/Shopify/sarama"
	"github.com/urfave/cli"
)

func ActionBaritoConsumerService(c *cli.Context) (err error) {
	brokers := configKafkaBrokers()
	groupID := configKafkaGroupId()
	esUrl := configElasticsearchUrl()
	topicSuffix := configKafkaTopicSuffix()
	newTopicEventName := configNewTopicEvent()

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_1 // TODO: get version from env

	factory := flow.NewKafkaFactory(brokers, config)

	service := flow.NewBaritoConsumerService(factory, groupID, esUrl, topicSuffix, newTopicEventName)

	if err = service.Start(); err != nil {
		return
	}

	srvkit.GracefullShutdown(service.Close)

	return
}

func ActionBaritoProducerService(c *cli.Context) (err error) {

	address := configProducerAddress()
	kafkaBrokers := configKafkaBrokers()
	maxRetry := configProducerMaxRetry()
	maxTps := configProducerMaxTPS()
	topicSuffix := configKafkaTopicSuffix()
	newTopicEventName := configNewTopicEvent()

	// kafka producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = maxRetry
	config.Producer.Return.Successes = true
	config.Version = sarama.V0_10_2_1 // TODO: get version from env

	factory := flow.NewKafkaFactory(kafkaBrokers, config)

	srv := flow.NewBaritoProducerServiceWithProtocol(
		flow.ProtocolSuperCharge,
		factory,
		address,
		maxTps,
		topicSuffix,
		newTopicEventName)

	err = srv.Start()
	if err != nil {
		return
	}
	srvkit.AsyncGracefulShutdown(srv.Close)

	return
}

// TODO: implement on consumer worker
// func callbackInstrumentation() bool {
// 	pushMetricUrl := configPushMetricUrl()
// 	pushMetricToken := configPushMetricToken()
// 	pushMetricInterval := configPushMetricInterval()
//
// 	if pushMetricToken == "" || pushMetricUrl == "" {
// 		log.Infof("No callback for instrumentation")
// 		return false
// 	}
//
// 	instru.SetCallback(
// 		timekit.Duration(pushMetricInterval),
// 		NewMetricMarketCallback(pushMetricUrl, pushMetricToken),
// 	)
// 	return true
//
// }
