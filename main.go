package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	pkg "github.com/quanghung97/kafka-go/pkg"
	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	KafkaUrl       string `default:"localhost:9092,localhost:9093,localhost:9094"`
	ProducerWriter *kafka.Writer
	ConsumerReader *kafka.Reader
}

// writer
func initKafkaWriter(k *Kafka, topic string) {
	addresses := strToArr(k.KafkaUrl)
	k.ProducerWriter = &kafka.Writer{
		Addr:                   pkg.MakeNetAddr("tcp", addresses),
		Topic:                  topic,
		AllowAutoTopicCreation: true, // enable auto create topic writer
		Logger:                 kafka.LoggerFunc(logf),
		ErrorLogger:            kafka.LoggerFunc(logf),
		// Balancer:               &kafka.LeastBytes{},
	}
}

func (k *Kafka) WriterSendMessage(topic string, key string, value string) error {
	if k.ProducerWriter == nil {
		initKafkaWriter(k, topic)
	} else {
		if k.ProducerWriter.Topic != topic {
			initKafkaWriter(k, topic)
		}
	}

	msg := kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	}

	// Missing topic creation before publication
	var err error
	const retries = 3
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// attempt to create topic prior to publishing the message
		err = k.ProducerWriter.WriteMessages(ctx, msg)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			fmt.Println("unexpected error", err)
		}
		break
	}
	return err
}

// reader init
func initKafkaReader(k *Kafka, topic string, groupID string) {
	brokers := strToArr(k.KafkaUrl)
	k.ConsumerReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    5,
		MaxBytes:    10e6, // 10MB
		MaxWait:     3 * time.Second,
		Logger:      kafka.LoggerFunc(logf),
		ErrorLogger: kafka.LoggerFunc(logf),
	})
}

func (k *Kafka) ReaderReceiveMessage(topic string, groupID string) (kafka.Message, error) {
	if k.ConsumerReader == nil {
		initKafkaReader(k, topic, groupID)
	} else {
		if k.ConsumerReader.Config().GroupID != groupID || k.ConsumerReader.Config().Topic != topic {
			initKafkaReader(k, topic, groupID)
		}
	}

	m, err := k.ConsumerReader.ReadMessage(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	return m, err
}

func logf(msg string, a ...interface{}) {
	fmt.Printf(msg, a...)
	fmt.Println()
}

func strToArr(url string) []string {
	return strings.Split(url, ",")
}
