package main

import (
	"fmt"
	"time"

	config "github.com/quanghung97/kafka-go"
)

// global config
var configKafka = &config.Kafka{
	KafkaUrl:          "localhost:9092",
	MinBytes:          5,
	MaxBytes:          10e6, // max 10MB
	MaxWait:           3 * time.Second,
	NumPartitions:     12,
	ReplicationFactor: 1,
}

func testConsumer(msg config.Message, err error) {
	fmt.Printf("message at topic:%v partition:%v %s = %s\n", msg.Topic, msg.Partition, string(msg.Key), string(msg.Value))
}

func testLog(msg string, a ...interface{}) {
	fmt.Printf(msg, a...)
	fmt.Println()
}

func main() {
	configKafka.ReaderReceiveMessage("topic-have-23", "log23", testConsumer, testLog)
}
