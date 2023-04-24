#Overview
https://drive.google.com/file/d/1PfMAxq1cAFu6fzV7NggYgvLmJXlz3ZVU/view?usp=sharing

# Producer

Example implement Producer

```
package main

import (
	"fmt"

	"github.com/google/uuid"
	config "github.com/quanghung97/kafka-go"
)

// global config
var configKafka = config.Kafka{
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
	// fmt.Println(constants.PACKAGE_KAFKA_WRITER_SEND_MESSAGE + "start producing ... !!")
	// for i := 0; ; i++ {
	// 	key := fmt.Sprintf("Key-%d", i)

	// 	fmt.Printf("created msg %d \n", i)
	// }
	configKafka.WriterSendMessage("topic-have-23", "123213", fmt.Sprint(uuid.New()), testLog)
	configKafka.WriterSendMessage("topic-have-23", "123214", fmt.Sprint(uuid.New()), testLog)
	configKafka.WriterSendMessage("topic-have-23", "123215", fmt.Sprint(uuid.New()), testLog)
	configKafka.WriterSendMessage("topic-have-23", "123216", fmt.Sprint(uuid.New()), testLog)
	configKafka.WriterSendMessage("topic-have-23", "123217", fmt.Sprint(uuid.New()), testLog)
	configKafka.WriterSendMessage("topic-have-23", "123218", fmt.Sprint(uuid.New()), testLog)
	configKafka.WriterSendMessage("topic-have-23", "123219", fmt.Sprint(uuid.New()), testLog)
	configKafka.WriterSendMessage("topic-have-23", "123221", fmt.Sprint(uuid.New()), testLog)
	configKafka.WriterSendMessage("topic-have-23", "123222", fmt.Sprint(uuid.New()), testLog)
	configKafka.WriterSendMessage("topic-have-23", "123223", fmt.Sprint(uuid.New()), testLog)
	configKafka.WriterSendMessage("topic-have-23", "123224", fmt.Sprint(uuid.New()), testLog)
	configKafka.WriterSendMessage("topic-have-23", "123225", fmt.Sprint(uuid.New()), testLog)
	configKafka.WriterSendMessage("topic-have-23", "123226", fmt.Sprint(uuid.New()), testLog)
	fmt.Println("end .......")
}
```

# Consumer

Example implement Consumer

```
package main

import (
	"fmt"
	"time"

	config "github.com/quanghung97/kafka-go"
)

// global config
var configKafka = config.Kafka{
	KafkaUrl: "localhost:9092",
	MinBytes: 5,
	MaxBytes: 10e6, // max 10MB
	MaxWait:  3 * time.Second,
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
```
# TEST

### run docker-compose
```
docker-compose up -d
```

### run consumer-logger

```
cd test/consumer-logger
go run main.go
```

### run producer-random

```
cd test/producer-random
go run main.go
```
