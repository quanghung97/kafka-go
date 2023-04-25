package main

import (
	"fmt"

	"github.com/google/uuid"
	config "github.com/quanghung97/kafka-go"
	"github.com/quanghung97/kafka-go/constants"
)

// global config
var configKafka = &config.Kafka{
	KafkaUrl:          "localhost:9092",
	NumPartitions:     12,
	ReplicationFactor: 1,
}

func testLog(msg string, a ...interface{}) {
	fmt.Printf(msg, a...)
	fmt.Println()
}

func main() {
	defer configKafka.ProducerWriter.Close()
	fmt.Println(constants.PACKAGE_KAFKA_WRITER_SEND_MESSAGE + "start producing ... !!")
	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		configKafka.WriterSendMessage("topic-have-23", key, fmt.Sprint(uuid.New()), testLog)
		fmt.Printf("\n created msg %d \n", i)
	}

}
