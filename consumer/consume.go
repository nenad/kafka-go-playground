package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/mum4k/termdash/widgets/segmentdisplay"
	"github.com/segmentio/kafka-go"
)

type ConsumerGroup struct {
	BrokerAddresses []string
	Topic           string
	GroupID         string
	Context         context.Context

	mu      sync.Mutex
	readers []*kafka.Reader
	logger  KLogger
}

func NewConsumerGroup(ctx context.Context, brokerAddresses []string, topic, groupID string, logger KLogger) ConsumerGroup {
	return ConsumerGroup{
		BrokerAddresses: brokerAddresses,
		Topic:           topic,
		GroupID:         groupID,
		Context:         ctx,
		readers:         make([]*kafka.Reader, 0),
		logger:          logger,
	}
}

func (g *ConsumerGroup) Count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.readers)
}

func (g *ConsumerGroup) AddReader() {
	g.mu.Lock()
	g.readers = append(g.readers, g.startNewReader(len(g.readers)))
	g.mu.Unlock()
}

func (g *ConsumerGroup) RemoveReader() {
	g.mu.Lock()
	if len(g.readers) == 0 {
		return
	}

	r := g.readers[0]
	if len(g.readers) > 1 {
		g.readers = g.readers[1:]
	} else {
		g.readers = make([]*kafka.Reader, 0)
	}
	g.mu.Unlock()

	go func() {
		if err := r.Close(); err != nil {
			g.logger.Printf("Failed to stop consumer\n")
		} else {
			g.logger.Printf("Removed reader\n")
		}
	}()
}

func (g *ConsumerGroup) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for i, r := range g.readers {
		if err := r.Close(); err != nil {
			return err
		}
		fmt.Printf("Closed %d reader\n", i)
	}
	fmt.Println("Closed all readers!!!")
	return nil
}

func (g *ConsumerGroup) startNewReader(workerID int) *kafka.Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: g.BrokerAddresses,
		GroupID: g.GroupID,
		Topic:   g.Topic,
	})

	defer func() {
		go func() {
			for {
				m, err := r.ReadMessage(g.Context)
				if err == io.EOF {
					g.logger.Printf("Consumer %d stopped: %s\n", workerID, err)
					break
				}
				if err != nil {
					g.logger.Printf("Consumer %d errored: %s\n", workerID, err)
					break
				}

				g.logger.Printf("--- Consumed from: %02d| Offset: %05d | Part: %02d ---\n", workerID, m.Offset, m.Partition)
			}
		}()
	}()

	return r
}

func main() {
	ctx := context.Background()
	if len(os.Args) < 2 {
		log.Fatalln("You must provide the broker urls")
	}

	kafkaBrokerUrls := os.Args[2:]

	for _, v := range kafkaBrokerUrls {
		_, err := kafka.DialContext(ctx, "tcp", v)
		if err != nil {
			panic(err)
		}
	}

	kui, err := NewKUI()
	if err != nil {
		log.Fatalln("Could not set up TUI", err)
	}

	cg := NewConsumerGroup(ctx, kafkaBrokerUrls, "products", "product-group", &kui)
	defer cg.Close()

	abort := make(chan os.Signal, 1)
	go func() {
		if err := kui.Run(ctx, func(display *segmentdisplay.SegmentDisplay) error {
			cg.AddReader()
			return kui.Write(strconv.Itoa(cg.Count()))
		}, func(display *segmentdisplay.SegmentDisplay) error {
			cg.RemoveReader()
			return kui.Write(strconv.Itoa(cg.Count()))
		}); err != nil {
			fmt.Printf("Failed to run KUI: %s", err)
		}
		abort <- syscall.SIGINT
	}()

	signal.Notify(abort, syscall.SIGTERM, syscall.SIGABRT, syscall.SIGINT)

	select {
	case <-abort:
		fmt.Println("Aborted CTRL+C")
		ctx.Done()
		break
	}

	fmt.Println("Closing all consumers...")
}
