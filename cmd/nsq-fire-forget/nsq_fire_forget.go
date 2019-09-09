package main

import (
	"log"
	"os"
	"os/signal"

	nsq_fire_forget "github.com/firdasafridi/example-nsq/internal/pkg/nsq-fire-forget"
)

func main() {
	nsqFireForget, err := nsq_fire_forget.New()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		errServer := nsqFireForget.Start()
		if errServer != nil {
			log.Fatal(errServer)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	nsqFireForget.Stop()
}
