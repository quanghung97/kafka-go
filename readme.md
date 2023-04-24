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
```

# Consumer

Example implement Consumer

```
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
