package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/quanghung97/kafka-go/constants"
	pkg "github.com/quanghung97/kafka-go/pkg"
	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	KafkaUrl          string        `default:"localhost:9092,localhost:9093,localhost:9094"`
	MinBytes          int           `default:"5"`
	MaxBytes          int           `default:"10e6"`
	MaxWait           time.Duration `default:"time.Second"`
	NumPartitions     int           `default:"12"`
	ReplicationFactor int           `default:"1"`
	ProducerWriter    *kafka.Writer
	ConsumerReader    *kafka.Reader
}

type Message struct {
	kafka.Message
}

// list topic
func (k *Kafka) topicIsExisted(topic string, conn *kafka.Conn) bool {
	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(constants.PACKAGE_KAFKA_GET_LIST_TOPIC + err.Error())
	}

	// m := []string{}
	check := false

	for _, p := range partitions {
		if p.Topic == topic {
			check = true
			break
		}
	}

	return check
}

// create topic
func (k *Kafka) CreateTopic(topic string, handlerError func(string, ...interface{})) {
	fmt.Printf(constants.PACKAGE_KAFKA_CREATE_TOPIC+"creating topic %s ... !! \n", topic)
	addresses := strToArr(k.KafkaUrl)
	conn, err := kafka.Dial("tcp", addresses[0])
	if err != nil {
		handlerError(constants.PACKAGE_KAFKA_CREATE_TOPIC + err.Error())
		panic(constants.PACKAGE_KAFKA_CREATE_TOPIC + err.Error())
	}
	defer conn.Close()

	// check is existed topic
	checked := k.topicIsExisted(topic, conn)

	if !checked {
		controller, err := conn.Controller()
		if err != nil {
			handlerError(constants.PACKAGE_KAFKA_CREATE_TOPIC + err.Error())
			panic(constants.PACKAGE_KAFKA_CREATE_TOPIC + err.Error())
		}
		var controllerConn *kafka.Conn
		controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
		if err != nil {
			handlerError(constants.PACKAGE_KAFKA_CREATE_TOPIC + err.Error())
			panic(constants.PACKAGE_KAFKA_CREATE_TOPIC + err.Error())
		}
		defer controllerConn.Close()

		topicConfigs := []kafka.TopicConfig{
			{
				Topic:             topic,
				NumPartitions:     k.NumPartitions,
				ReplicationFactor: k.ReplicationFactor,
			},
		}

		err = controllerConn.CreateTopics(topicConfigs...)
		if err != nil {
			handlerError(constants.PACKAGE_KAFKA_CREATE_TOPIC + err.Error())
			panic(constants.PACKAGE_KAFKA_CREATE_TOPIC + err.Error())
		} else {
			fmt.Printf(constants.PACKAGE_KAFKA_CREATE_TOPIC+"create topic %s successfully !! \n", topic)
		}
	} else {
		fmt.Printf(constants.PACKAGE_KAFKA_CREATE_TOPIC+"topic %s existed !! \n", topic)
	}
}

// writer
func initKafkaWriter(k *Kafka, topic string, handlerError func(string, ...interface{})) {
	addresses := strToArr(k.KafkaUrl)
	// auto create topic
	k.CreateTopic(topic, handlerError)
	// instance writer
	k.ProducerWriter = &kafka.Writer{
		Addr:                   pkg.MakeNetAddr("tcp", addresses),
		Topic:                  topic,
		AllowAutoTopicCreation: false, // disable auto create topic writer
		// Logger:                 kafka.LoggerFunc(handlerError),
		ErrorLogger: kafka.LoggerFunc(handlerError),
	}
}

func (k *Kafka) WriterSendMessage(topic string, key string, value string, handlerError func(string, ...interface{})) {
	if k.ProducerWriter == nil {
		initKafkaWriter(k, topic, handlerError)
	} else {
		if k.ProducerWriter.Topic != topic {
			initKafkaWriter(k, topic, handlerError)
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
			fmt.Println(constants.PACKAGE_KAFKA_WRITER_SEND_MESSAGE + "unexpected error" + err.Error())
			handlerError(constants.PACKAGE_KAFKA_WRITER_SEND_MESSAGE + "unexpected error" + err.Error())
		} else {
			break
		}
	}
}

// reader init
func initKafkaReader(k *Kafka, topic string, groupID string, handlerError func(string, ...interface{})) {
	// auto create topic
	k.CreateTopic(topic, handlerError)
	brokers := strToArr(k.KafkaUrl)
	k.ConsumerReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: k.MinBytes,
		MaxBytes: k.MaxBytes, // max 10MB
		MaxWait:  k.MaxWait,
		// Logger:                 kafka.LoggerFunc(handlerError),
		ErrorLogger: kafka.LoggerFunc(handlerError),
	})
}

func (k *Kafka) ReaderReceiveMessage(topic string, groupID string, callback func(Message, error), handlerError func(string, ...interface{})) {
	defer k.ConsumerReader.Close()
	fmt.Println(constants.PACKAGE_KAFKA_READER_RECEIVE_MESSAGE + "start consuming ... !!")
	for {
		if k.ConsumerReader == nil {
			initKafkaReader(k, topic, groupID, handlerError)
		} else {
			if k.ConsumerReader.Config().GroupID != groupID || k.ConsumerReader.Config().Topic != topic {
				initKafkaReader(k, topic, groupID, handlerError)
			}
		}

		msg, err := k.ConsumerReader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(constants.PACKAGE_KAFKA_READER_RECEIVE_MESSAGE + err.Error())
			handlerError(constants.PACKAGE_KAFKA_READER_RECEIVE_MESSAGE + err.Error())
			break
		}
		callback(Message{msg}, err)
	}
	if err := k.ConsumerReader.Close(); err != nil {
		fmt.Println(constants.PACKAGE_KAFKA_READER_RECEIVE_MESSAGE + "failed to close reader" + err.Error())
		handlerError(constants.PACKAGE_KAFKA_READER_RECEIVE_MESSAGE + "failed to close reader" + err.Error())
	}
}

// func logf(msg string, a ...interface{}) {
// 	fmt.Printf(msg, a...)
// 	fmt.Println()
// }

func strToArr(url string) []string {
	return strings.Split(url, ",")
}
