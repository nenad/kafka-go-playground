package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalln("You must provide the broker url")
	}

	kafkaBrokerUrl := os.Args[1]

	ctx := context.Background()
	c, err := kafka.DialContext(ctx, "tcp", kafkaBrokerUrl)
	if err != nil {
		panic(err)
	}

	if err := c.CreateTopics(kafka.TopicConfig{Topic: "products", NumPartitions: 10, ReplicationFactor: 2}); err != nil {
		panic(err)
	}

	fmt.Print("Done!")
}
