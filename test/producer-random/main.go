package main

import (
	"fmt"

	"github.com/google/uuid"
	config "github.com/quanghung97/kafka-go"
)

// global config
var k = config.Kafka{
	KafkaUrl: "localhost:9092",
}

func main() {
	defer k.ProducerWriter.Close()

	fmt.Println("start producing ... !!")
	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		k.WriterSendMessage("services10", key, fmt.Sprint(uuid.New()))
	}
}
