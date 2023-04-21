# Producer

Example implement Producer

```
package main

import (
	"fmt"

	"github.com/google/uuid"
	config "github.com/quanghung97/kafka-go"
)

func main() {
    // init config
	k := config.Kafka{
		KafkaUrl: "localhost:9092,localhost:9093,localhost:9094",
	}
    // init writer
	k.InitKafkaWriter("topic1")

    // defer handler writer errors
	defer k.ProducerWriter.Close()

	fmt.Println("start producing ... !!")
	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		k.WriterSendMessage(key, fmt.Sprint(uuid.New()))
	}
}
```

# Consumer

Example implement Consumer

```
package main

import (
	"fmt"

	config "github.com/quanghung97/kafka-go"
)

func main() {
	k := config.Kafka{
		KafkaUrl: "localhost:9092",
	}
	k.InitKafkaReader("topic1", "group-logger")
	defer k.ConsumerReader.Close()
	fmt.Println("start consuming ... !!")
	for {
		m, err := k.ReaderReceiveMessage()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
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
