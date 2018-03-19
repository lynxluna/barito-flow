package cmds

import (
	"github.com/BaritoLog/barito-flow/river"
	"github.com/urfave/cli"

	log "github.com/sirupsen/logrus"
)

func Receiver(c *cli.Context) (err error) {

	conf, err := NewReceiverConfigByEnv()
	if err != nil {
		return
	}
	conf.Info(log.StandardLogger())

	receiver, err := conf.ReceiverUpstream()
	if err != nil {
		return
	}

	kafka, err := conf.KafkaDownstream()
	if err != nil {
		return
	}

	raft := river.NewRaft(receiver, kafka)
	raft.Start()

	errCh := raft.ErrorChannel()

	for {
		select {
		case err = <-errCh:
			return err
		}
	}

	return

}