package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()
	if len(os.Args) < 2 {
		log.Fatalln("You must provide the broker urls")
	}

	kafkaBrokerUrls := os.Args[1:]

	for _, url := range kafkaBrokerUrls {
		_, err := kafka.DialContext(ctx, "tcp", url)
		if err != nil {
			log.Fatalln("Failed to dial to server", url, err)
		}
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: kafkaBrokerUrls,
		Topic:   "products",
	})
	
	
	abort := make(chan os.Signal, 1)
	signal.Notify(abort, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)

	r := bufio.NewReader(os.Stdin)
	go func() {
		for {
			fmt.Println("Enter how many messages to be published to topic 'products'")
			data, err := r.ReadString('\n')
			if err != nil {
				log.Fatalln(err)
			}


			count, _ := strconv.Atoi(strings.TrimSpace(data))
			if count == 0 {
				fmt.Println("No messages will be published.")
				continue
			}
			fmt.Printf("Publishing %d messages\n", count)

			finishWg := sync.WaitGroup{}
			finishWg.Add(count)
			beginTime := time.Now()
			for i := 0; i < count; i++ {
				go func(i int) {
					err := w.WriteMessages(
						ctx,
						kafka.Message{
							Topic: "products",
							Value: []byte(fmt.Sprintf("Message number %d", i)),
							Headers: []kafka.Header{{
								Key:   "Type",
								Value: []byte("raw-message"),
							}}},
					)

					if err != nil {
						fmt.Printf("Error for message %d, %s", i, err)
					}
					finishWg.Done()
				}(i)
			}
			finishWg.Wait()
			fmt.Println("Took: ", time.Now().Sub(beginTime).Milliseconds(), "ms")
		}
	}()
	

	<-abort
	
	w.Close()
	ctx.Done()
	fmt.Println("Closed writer.")
}
