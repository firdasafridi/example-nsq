package nsq_fire_forget

import (
	"log"

	"github.com/nsqio/go-nsq"

	platform "github.com/firdasafridi/example-nsq"
)

type NsqFireForget struct {
	consumer *nsq.Consumer
	env      *platform.Environment
}

func (nsqFireForget *NsqFireForget) Start() (err error) {
	log.Println("Start example nsq fire n forget consumer")
	return nil
}

func (nsqFireForget *NsqFireForget) Stop() {
	log.Println("Stop example nsq fire n forget")
}

func New() (nsqFireForget *NsqFireForget, err error) {
	nsqFireForget = &NsqFireForget{}

	nsqFireForget.env = platform.NewEnvironment()

	err = nsqFireForget.NsqHandler()
	if err != nil {
		log.Println(err)
	}
	return nsqFireForget, nil
}

func (nsqFireForget *NsqFireForget) NsqHandler() (err error) {
	config := nsq.NewConfig()
	consum, err := nsq.NewConsumer("write_test", "ch", config)
	if err != nil {
		return err
	}

	consum.AddHandler(nsq.HandlerFunc(message))
	err = consum.ConnectToNSQD(nsqFireForget.env.Nsq.Host)
	if err != nil {
		return err
	}
	return nil
}

func message(message *nsq.Message) error {
	log.Printf("Got a message: %v\n", message)
	log.Printf("The message %s\n", string(message.Body))
	return nil
}
