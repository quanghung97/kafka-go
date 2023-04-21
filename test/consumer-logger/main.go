package main

import (
	"fmt"

	config "github.com/quanghung97/kafka-go"
)

// global config
var k = config.Kafka{
	KafkaUrl: "localhost:9092,localhost:9093,localhost:9094",
}

func main() {
	defer k.ConsumerReader.Close()

	fmt.Println("start consuming ... !!")
	for {
		m, err := k.ReaderReceiveMessage("topic1", "group-logger")
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}
