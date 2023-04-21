package main

import (
	"fmt"

	"github.com/google/uuid"
	config "github.com/quanghung97/kafka-go"
)

func main() {
	k := config.Kafka{
		KafkaUrl: "localhost:9092,localhost:9093,localhost:9094",
	}
	k.InitKafkaWriter("topic1")

	defer k.ProducerWriter.Close()

	fmt.Println("start producing ... !!")
	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		k.WriterSendMessage(key, fmt.Sprint(uuid.New()))
	}
}
