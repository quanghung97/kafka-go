package main

import (
	"fmt"

	"github.com/google/uuid"
	config "github.com/quanghung97/kafka-go"
)

// global config
var k = config.Kafka{
	KafkaUrl: "localhost:9092,localhost:9093,localhost:9094",
}

func main() {
	defer k.ProducerWriter.Close()

	fmt.Println("start producing ... !!")
	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		k.WriterSendMessage("topic1", key, fmt.Sprint(uuid.New()))
	}
}
