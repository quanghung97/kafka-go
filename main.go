package kafka

import (
	"context"
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

func strToArr(url string) []string {
	return strings.Split(url, ",")
}

// writer
func (k *Kafka) InitKafkaWriter(topic string) {
	addresses := strToArr(k.KafkaUrl)
	k.ProducerWriter = &kafka.Writer{
		Addr:     pkg.MakeNetAddr("tcp", addresses),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func (k *Kafka) WriterSendMessage(key string, value string) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	}

	err := k.ProducerWriter.WriteMessages(context.Background(), msg)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("produced", key)
	}
	return err
}

// reader init
func (k *Kafka) InitKafkaReader(topic string, groupID string) {
	brokers := strToArr(k.KafkaUrl)
	k.ConsumerReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 5,    // 1KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  3 * time.Second,
	})
}

func (k *Kafka) ReaderReceiveMessage() (kafka.Message, error) {
	m, err := k.ConsumerReader.ReadMessage(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	// fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	return m, err
}
