package main

import (
	"fmt"

	config "github.com/quanghung97/kafka-go"
)

// global config
var k = config.Kafka{
	KafkaUrl: "localhost:9092",
}

func main() {
	defer k.ConsumerReader.Close()

	fmt.Println("start consuming ... !!")
	for {
		_, err := k.ReaderReceiveMessage("services10", "group-logger")
		if err != nil {
			fmt.Println(err)
			break
		}
		// fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	if err := k.ConsumerReader.Close(); err != nil {
		fmt.Println("failed to close reader:", err)
	}
}
